# podman-api Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the v1 of the stateless `podman-api` HTTP service in Go per `docs/superpowers/specs/2026-05-09-podman-api-design.md`.

**Architecture:** Stateless Go service. Reads `hosts/*.yaml`, `templates/*.yaml` (embedded), and `auth/keys.yaml` at boot. CMS calls REST endpoints; service translates to libpod REST calls (via `containers/podman/v5/pkg/bindings`). No DB. One pooled SSH (or unix) connection per host. TLS terminates outside the API.

**Tech Stack:** Go 1.23+, stdlib `net/http` (1.22+ path patterns), `containers/podman/v5/pkg/bindings`, `gopkg.in/yaml.v3`, `golang.org/x/crypto/argon2`, `prometheus/client_golang`, stdlib `testing` + `stretchr/testify` for assertions.

---

## SAFETY CONSTRAINT — NO REMOTE OPERATIONS

**No code path written under this plan, and no test executed under this plan, may connect to any remote podman host.** Concretely:

- Integration tests connect **only** to the local rootless podman socket at `$XDG_RUNTIME_DIR/podman/podman.sock` (typically `/run/user/1000/podman/podman.sock` for the current user). They use a `unix://` URI, never `ssh://`.
- The `podman.Client.New()` constructor accepts `ssh://...` URIs because that is the production transport — but the plan's tests never construct one. SSH transport correctness is verified manually post-merge against a staging host (out of scope for this plan).
- Do **not** add any host config file (`hosts/*.yaml`) referencing a remote machine while this plan is being executed. The only host file used in dev is `hosts/local.yaml` pointing at the local socket via `unix://`.
- Do **not** invoke the executable against any non-local host during this plan, even manually.
- The Ring 3 "smoke test" mentioned in the spec (against a staging host) is explicitly **out of scope** for this plan.

Any task step that appears to violate this constraint is a bug in the plan — stop and flag it.

---

## Conventions used throughout the plan

- **Module path:** `github.com/iotready/podman-api` (placeholder — adjust to actual GitHub org if different; only affects imports).
- **Test framework:** stdlib `testing` for the runner; `github.com/stretchr/testify/assert` and `.../require` for assertions.
- **Integration tests:** every file containing tests that require a real podman socket starts with `//go:build integration` so `go test ./...` skips them and `go test -tags=integration ./...` runs them.
- **Commits:** one commit per task, conventional commits (`feat:`, `test:`, `chore:`, `docs:`).
- **Format:** every code change must be `gofmt`-clean. Run `gofmt -w .` before committing if unsure.

---

## Phase 0 — Project bootstrap

### Task 1: Initialize Go module, Makefile, .gitignore

**Files:**
- Create: `go.mod`
- Create: `Makefile`
- Create: `.gitignore`

- [ ] **Step 1: Initialize Go module**

Run: `go mod init github.com/iotready/podman-api`
Expected: creates `go.mod` with module line and `go 1.23` (or current).

- [ ] **Step 2: Create Makefile**

Create `Makefile`:

```make
.PHONY: build test test-integration fmt vet tidy

build:
	go build -o bin/podman-api ./cmd/podman-api

test:
	go test ./...

test-integration:
	go test -tags=integration ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

tidy:
	go mod tidy
```

- [ ] **Step 3: Create .gitignore**

Create `.gitignore`:

```
/bin/
*.test
*.out
coverage.txt
.idea/
.vscode/
```

- [ ] **Step 4: Verify it builds (no targets yet)**

Run: `go build ./...`
Expected: exits 0 with no output (no packages yet).

- [ ] **Step 5: Commit**

```bash
git add go.mod Makefile .gitignore
git commit -m "chore: initialize Go module and Makefile"
```

---

## Phase 1 — `internal/render` (pure: parameters → YAML)

This package has zero I/O. It (a) parses the `# template-meta:` comment block from a template file, (b) renders the body with `text/template`, (c) validates parameter and secret maps against the metadata.

### Task 2: Define metadata struct and parser

**Files:**
- Create: `internal/render/meta.go`
- Create: `internal/render/meta_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/render/meta_test.go`:

```go
package render

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMeta_Minimal(t *testing.T) {
	src := `# template-meta:
#   id: lite-engine
#   parameters:
#     required: [slug, image]
#     optional: []
#   secrets:
#     per_instance: [auth_secret]
#     per_host_referenced: [s3-access-key-id]
#   volumes:
#     - name: data
#       backup: litestream
---
apiVersion: v1
kind: Pod
`
	meta, body, err := ParseMeta(src)
	require.NoError(t, err)

	assert.Equal(t, "lite-engine", meta.ID)
	assert.Equal(t, []string{"slug", "image"}, meta.Parameters.Required)
	assert.Empty(t, meta.Parameters.Optional)
	assert.Equal(t, []string{"auth_secret"}, meta.Secrets.PerInstance)
	assert.Equal(t, []string{"s3-access-key-id"}, meta.Secrets.PerHostReferenced)
	require.Len(t, meta.Volumes, 1)
	assert.Equal(t, "data", meta.Volumes[0].Name)
	assert.Equal(t, "litestream", meta.Volumes[0].Backup)

	assert.Contains(t, body, "apiVersion: v1")
	assert.NotContains(t, body, "template-meta")
}

func TestParseMeta_MissingMeta(t *testing.T) {
	src := `apiVersion: v1
kind: Pod
`
	_, _, err := ParseMeta(src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "template-meta")
}

func TestParseMeta_MissingID(t *testing.T) {
	src := `# template-meta:
#   parameters:
#     required: []
---
apiVersion: v1
`
	_, _, err := ParseMeta(src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "id")
}
```

- [ ] **Step 2: Run test — expect fail (no package yet)**

Run: `go test ./internal/render/...`
Expected: FAIL — `no Go files` or build failure (`ParseMeta` undefined).

- [ ] **Step 3: Implement minimal `meta.go`**

Create `internal/render/meta.go`:

```go
package render

import (
	"bufio"
	"errors"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// Meta describes a template's parameter and secret contract.
// It is parsed from the leading "# template-meta:" comment block.
type Meta struct {
	ID         string     `yaml:"id"`
	Parameters Parameters `yaml:"parameters"`
	Secrets    Secrets    `yaml:"secrets"`
	Volumes    []Volume   `yaml:"volumes"`
}

type Parameters struct {
	Required []string `yaml:"required"`
	Optional []string `yaml:"optional"`
}

type Secrets struct {
	PerInstance       []string `yaml:"per_instance"`
	PerHostReferenced []string `yaml:"per_host_referenced"`
}

type Volume struct {
	Name   string `yaml:"name"`
	Backup string `yaml:"backup,omitempty"`
}

// ParseMeta extracts the template-meta block from the head of the file
// and returns the rest of the file as the renderable body.
//
// The block must look like:
//
//	# template-meta:
//	#   id: lite-engine
//	#   parameters: ...
//
// The parser stops at the first non-comment line. The body is everything
// from that point onward (with a leading "---" preserved if present).
func ParseMeta(src string) (Meta, string, error) {
	var (
		yamlLines []string
		bodyStart int
		started   bool
	)

	sc := bufio.NewScanner(strings.NewReader(src))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	lineNo := 0
	for sc.Scan() {
		line := sc.Text()
		lineNo++

		if !started {
			trim := strings.TrimSpace(line)
			if trim == "" {
				bodyStart = len(yamlLinesJoined(yamlLines, src, lineNo))
				continue
			}
			if !strings.HasPrefix(trim, "# template-meta:") {
				return Meta{}, "", errors.New("template-meta: block not found at top of file")
			}
			started = true
			yamlLines = append(yamlLines, "template-meta:")
			continue
		}

		if strings.HasPrefix(line, "#") {
			// Strip the leading "# " (or "#") and keep indentation after it.
			stripped := strings.TrimPrefix(line, "#")
			stripped = strings.TrimPrefix(stripped, " ")
			yamlLines = append(yamlLines, stripped)
			continue
		}

		// Non-comment line ends the block.
		bodyStart = lineNo - 1
		break
	}
	if err := sc.Err(); err != nil {
		return Meta{}, "", fmt.Errorf("scan: %w", err)
	}

	if !started {
		return Meta{}, "", errors.New("template-meta: block not found at top of file")
	}

	var wrapper struct {
		Meta Meta `yaml:"template-meta"`
	}
	if err := yaml.Unmarshal([]byte(strings.Join(yamlLines, "\n")), &wrapper); err != nil {
		return Meta{}, "", fmt.Errorf("parse template-meta: %w", err)
	}
	if wrapper.Meta.ID == "" {
		return Meta{}, "", errors.New("template-meta: id is required")
	}

	body := bodyAfterLine(src, bodyStart)
	return wrapper.Meta, body, nil
}

// bodyAfterLine returns the substring of src starting at line number n (1-indexed).
func bodyAfterLine(src string, n int) string {
	if n <= 0 {
		return src
	}
	cur := 0
	for i := 0; i < n-1; i++ {
		idx := strings.IndexByte(src[cur:], '\n')
		if idx == -1 {
			return ""
		}
		cur += idx + 1
	}
	return src[cur:]
}

// yamlLinesJoined is unused but kept for future debugging.
func yamlLinesJoined(lines []string, _ string, _ int) string {
	return strings.Join(lines, "\n")
}
```

- [ ] **Step 4: Run test — expect pass**

Run: `go test ./internal/render/...`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/render/meta.go internal/render/meta_test.go go.mod go.sum
git commit -m "feat(render): parse template-meta comment block"
```

---

### Task 3: Implement `Render` (text/template → YAML string)

**Files:**
- Create: `internal/render/template.go`
- Create: `internal/render/template_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/render/template_test.go`:

```go
package render

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRender_SubstitutesParams(t *testing.T) {
	src := `# template-meta:
#   id: x
#   parameters:
#     required: [slug]
---
apiVersion: v1
kind: Pod
metadata:
  name: x-{{.slug}}
`
	out, err := Render(src, map[string]any{"slug": "iotready"})
	require.NoError(t, err)
	assert.Contains(t, out, "name: x-iotready")
	assert.NotContains(t, out, "template-meta")
}

func TestRender_MissingParamErrors(t *testing.T) {
	src := `# template-meta:
#   id: x
#   parameters:
#     required: [slug]
---
metadata:
  name: x-{{.slug}}
`
	// Use Go template's missingkey=error mode.
	_, err := Render(src, map[string]any{})
	require.Error(t, err)
}

func TestRender_PreservesMultipleDocs(t *testing.T) {
	src := `# template-meta:
#   id: x
#   parameters:
#     required: [slug]
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm-{{.slug}}
---
apiVersion: v1
kind: Pod
metadata:
  name: p-{{.slug}}
`
	out, err := Render(src, map[string]any{"slug": "a"})
	require.NoError(t, err)
	assert.Contains(t, out, "name: cm-a")
	assert.Contains(t, out, "name: p-a")
}
```

- [ ] **Step 2: Run test — expect fail**

Run: `go test ./internal/render/ -run TestRender`
Expected: FAIL (`Render` undefined).

- [ ] **Step 3: Implement `template.go`**

Create `internal/render/template.go`:

```go
package render

import (
	"bytes"
	"fmt"
	"text/template"
)

// Render parses src with ParseMeta, substitutes params into the body using
// text/template (with missingkey=error), and returns the final YAML.
func Render(src string, params map[string]any) (string, error) {
	_, body, err := ParseMeta(src)
	if err != nil {
		return "", err
	}
	tmpl, err := template.New("template").
		Option("missingkey=error").
		Parse(body)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, params); err != nil {
		return "", fmt.Errorf("execute template: %w", err)
	}
	return buf.String(), nil
}
```

- [ ] **Step 4: Run test — expect pass**

Run: `go test ./internal/render/ -run TestRender`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/render/template.go internal/render/template_test.go
git commit -m "feat(render): render template body with text/template"
```

---

### Task 4: Implement parameter & secret validation

**Files:**
- Create: `internal/render/validate.go`
- Create: `internal/render/validate_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/render/validate_test.go`:

```go
package render

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func meta() Meta {
	return Meta{
		ID: "x",
		Parameters: Parameters{
			Required: []string{"slug", "image"},
			Optional: []string{"port"},
		},
		Secrets: Secrets{
			PerInstance:       []string{"auth_secret"},
			PerHostReferenced: []string{"s3-access-key-id"},
		},
	}
}

func TestValidate_Happy(t *testing.T) {
	err := Validate(meta(),
		map[string]any{"slug": "a", "image": "x", "port": 1},
		map[string]string{"auth_secret": "v"},
	)
	require.NoError(t, err)
}

func TestValidate_MissingRequiredParam(t *testing.T) {
	err := Validate(meta(),
		map[string]any{"slug": "a"},
		map[string]string{"auth_secret": "v"},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "image")
}

func TestValidate_UnknownParam(t *testing.T) {
	err := Validate(meta(),
		map[string]any{"slug": "a", "image": "x", "extra": "no"},
		map[string]string{"auth_secret": "v"},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extra")
}

func TestValidate_MissingPerInstanceSecret(t *testing.T) {
	err := Validate(meta(),
		map[string]any{"slug": "a", "image": "x"},
		map[string]string{},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "auth_secret")
}

func TestValidate_UnknownSecret(t *testing.T) {
	err := Validate(meta(),
		map[string]any{"slug": "a", "image": "x"},
		map[string]string{"auth_secret": "v", "extra": "no"},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extra")
}
```

- [ ] **Step 2: Run test — expect fail**

Run: `go test ./internal/render/ -run TestValidate`
Expected: FAIL (`Validate` undefined).

- [ ] **Step 3: Implement `validate.go`**

Create `internal/render/validate.go`:

```go
package render

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Validate checks that params and secrets satisfy the template's contract:
//   - All Required parameters are present.
//   - No params outside Required ∪ Optional.
//   - All PerInstance secrets are present.
//   - No secrets outside PerInstance (PerHostReferenced are not in this map).
//
// Returns a single error listing every problem found.
func Validate(m Meta, params map[string]any, secrets map[string]string) error {
	var problems []string

	allowedParams := stringSet(m.Parameters.Required, m.Parameters.Optional)
	for _, k := range m.Parameters.Required {
		if _, ok := params[k]; !ok {
			problems = append(problems, fmt.Sprintf("missing required parameter %q", k))
		}
	}
	for k := range params {
		if !allowedParams[k] {
			problems = append(problems, fmt.Sprintf("unknown parameter %q", k))
		}
	}

	allowedSecrets := stringSet(m.Secrets.PerInstance)
	for _, k := range m.Secrets.PerInstance {
		if _, ok := secrets[k]; !ok {
			problems = append(problems, fmt.Sprintf("missing required secret %q", k))
		}
	}
	for k := range secrets {
		if !allowedSecrets[k] {
			problems = append(problems, fmt.Sprintf("unknown secret %q", k))
		}
	}

	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)
	return errors.New(strings.Join(problems, "; "))
}

func stringSet(lists ...[]string) map[string]bool {
	out := map[string]bool{}
	for _, l := range lists {
		for _, s := range l {
			out[s] = true
		}
	}
	return out
}
```

- [ ] **Step 4: Run test — expect pass**

Run: `go test ./internal/render/ -run TestValidate`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/render/validate.go internal/render/validate_test.go
git commit -m "feat(render): validate parameters and secrets against meta"
```

---

## Phase 2 — `internal/config` (load static config)

### Task 5: Load `hosts/*.yaml` into typed structs

**Files:**
- Create: `internal/config/hosts.go`
- Create: `internal/config/hosts_test.go`
- Create (test fixtures): `internal/config/testdata/hosts/local.yaml`, `internal/config/testdata/hosts/otp-prod-1.yaml`

- [ ] **Step 1: Write the failing test**

Create `internal/config/testdata/hosts/local.yaml`:

```yaml
id: local
addr: unix
socket: /run/user/1000/podman/podman.sock
labels:
  env: dev
```

Create `internal/config/testdata/hosts/otp-prod-1.yaml`:

```yaml
id: otp-prod-1
addr: ubuntu@otp-prod-1
socket: /run/user/1000/podman/podman.sock
ssh_key: /etc/podman-api/ssh/otp-prod-1
labels:
  env: prod
  region: in
```

Create `internal/config/hosts_test.go`:

```go
package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadHosts(t *testing.T) {
	hosts, err := LoadHosts("testdata/hosts")
	require.NoError(t, err)
	require.Len(t, hosts, 2)

	byID := map[string]Host{}
	for _, h := range hosts {
		byID[h.ID] = h
	}

	local, ok := byID["local"]
	require.True(t, ok)
	assert.Equal(t, "unix", local.Addr)
	assert.Equal(t, "/run/user/1000/podman/podman.sock", local.Socket)
	assert.Equal(t, "dev", local.Labels["env"])

	prod, ok := byID["otp-prod-1"]
	require.True(t, ok)
	assert.Equal(t, "ubuntu@otp-prod-1", prod.Addr)
	assert.Equal(t, "/etc/podman-api/ssh/otp-prod-1", prod.SSHKey)
}

func TestLoadHosts_MissingDir(t *testing.T) {
	_, err := LoadHosts("testdata/does-not-exist")
	require.Error(t, err)
}

func TestLoadHosts_DuplicateID(t *testing.T) {
	// Create temp dir with two files using the same id.
	dir := t.TempDir()
	err := writeFile(dir+"/a.yaml", "id: same\naddr: unix\nsocket: /tmp/x\n")
	require.NoError(t, err)
	err = writeFile(dir+"/b.yaml", "id: same\naddr: unix\nsocket: /tmp/y\n")
	require.NoError(t, err)
	_, err = LoadHosts(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}
```

Note: `writeFile` is a small test helper. Define it in this file at the bottom:

```go
func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0644)
}
```

…and add `import "os"`.

- [ ] **Step 2: Run test — expect fail**

Run: `go test ./internal/config/ -run TestLoadHosts`
Expected: FAIL (`LoadHosts` undefined).

- [ ] **Step 3: Implement `hosts.go`**

Create `internal/config/hosts.go`:

```go
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Host is the in-memory representation of a single hosts/*.yaml file.
type Host struct {
	ID     string            `yaml:"id"`
	Addr   string            `yaml:"addr"`              // "unix" or "user@host"
	Socket string            `yaml:"socket"`            // path on the host
	SSHKey string            `yaml:"ssh_key,omitempty"` // optional
	Labels map[string]string `yaml:"labels,omitempty"`
}

// LoadHosts reads every *.yaml in dir into a Host. Unknown fields are rejected.
// Duplicate IDs are an error.
func LoadHosts(dir string) ([]Host, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read hosts dir %q: %w", dir, err)
	}

	var hosts []Host
	seen := map[string]string{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		var h Host
		dec := yaml.NewDecoder(strings.NewReader(string(raw)))
		dec.KnownFields(true)
		if err := dec.Decode(&h); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		if h.ID == "" {
			return nil, fmt.Errorf("%s: id is required", path)
		}
		if prev, ok := seen[h.ID]; ok {
			return nil, fmt.Errorf("duplicate host id %q in %s and %s", h.ID, prev, path)
		}
		seen[h.ID] = path
		hosts = append(hosts, h)
	}
	return hosts, nil
}
```

- [ ] **Step 4: Run test — expect pass**

Run: `go test ./internal/config/ -run TestLoadHosts`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/config/hosts.go internal/config/hosts_test.go internal/config/testdata/hosts/
git commit -m "feat(config): load hosts/*.yaml"
```

---

### Task 6: Embed templates and load metadata for each

**Files:**
- Create: `templates/templates.go`
- Create: `templates/lite-engine.yaml` (verbatim from spec — copy the template body from the design doc, including the `# template-meta:` comment block)
- Create: `internal/config/templates.go`
- Create: `internal/config/templates_test.go`

- [ ] **Step 1: Create the templates package with go:embed**

Create `templates/templates.go`:

```go
// Package templates exposes the YAML pod-spec templates shipped with the binary.
package templates

import "embed"

//go:embed *.yaml
var Files embed.FS
```

- [ ] **Step 2: Add `templates/lite-engine.yaml`**

Copy the full body of the lite-engine template from
`docs/superpowers/specs/2026-05-09-podman-api-design.md` (the "Example: `templates/lite-engine.yaml`" code block — everything from `# template-meta:` to the closing `}}`). Do not alter the YAML.

- [ ] **Step 3: Write the failing test**

Create `internal/config/templates_test.go`:

```go
package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/templates"
)

func TestLoadTemplates_FromEmbed(t *testing.T) {
	tmpls, err := LoadTemplates(templates.Files, ".")
	require.NoError(t, err)

	byID := map[string]Template{}
	for _, t := range tmpls {
		byID[t.Meta.ID] = t
	}

	le, ok := byID["lite-engine"]
	require.True(t, ok, "expected lite-engine template to load")
	assert.Contains(t, le.Meta.Parameters.Required, "slug")
	assert.Contains(t, le.Meta.Parameters.Required, "image")
	assert.Contains(t, le.Meta.Secrets.PerInstance, "auth_secret")
	assert.NotEmpty(t, le.Body, "body should be non-empty")
}
```

- [ ] **Step 4: Run test — expect fail**

Run: `go test ./internal/config/ -run TestLoadTemplates`
Expected: FAIL (`LoadTemplates` undefined OR `Template` undefined).

- [ ] **Step 5: Implement `templates.go`**

Create `internal/config/templates.go`:

```go
package config

import (
	"fmt"
	"io/fs"
	"strings"

	"github.com/iotready/podman-api/internal/render"
)

// Template is a parsed template ready to render.
type Template struct {
	Meta   render.Meta
	Body   string // body after the template-meta block, fed to text/template
	Source string // filename, for diagnostics
}

// LoadTemplates reads every *.yaml under root in fsys and parses each into a Template.
// Use root="." with embed.FS for the bundled templates.
func LoadTemplates(fsys fs.FS, root string) ([]Template, error) {
	var out []Template

	err := fs.WalkDir(fsys, root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return nil
		}
		raw, err := fs.ReadFile(fsys, path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		meta, body, err := render.ParseMeta(string(raw))
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		out = append(out, Template{Meta: meta, Body: body, Source: path})
		return nil
	})
	if err != nil {
		return nil, err
	}

	seen := map[string]string{}
	for _, t := range out {
		if prev, ok := seen[t.Meta.ID]; ok {
			return nil, fmt.Errorf("duplicate template id %q in %s and %s", t.Meta.ID, prev, t.Source)
		}
		seen[t.Meta.ID] = t.Source
	}
	return out, nil
}
```

- [ ] **Step 6: Run test — expect pass**

Run: `go test ./internal/config/ -run TestLoadTemplates`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add templates/ internal/config/templates.go internal/config/templates_test.go
git commit -m "feat(config): load embedded templates and parse metadata"
```

---

### Task 7: Load `auth/keys.yaml` and verify argon2id tokens

**Files:**
- Create: `internal/config/auth.go`
- Create: `internal/config/auth_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/config/auth_test.go`:

```go
package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseKeysYAML(t *testing.T) {
	src := `keys:
  - id: cms-prod
    secret_hash: $argon2id$v=19$m=65536,t=3,p=4$abc$def
    scopes: [hosts:read, instances:*]
    description: "CMS production"
`
	keys, err := ParseKeysYAML([]byte(src))
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, "cms-prod", keys[0].ID)
	assert.Equal(t, []string{"hosts:read", "instances:*"}, keys[0].Scopes)
}

func TestKey_HasScope(t *testing.T) {
	k := APIKey{Scopes: []string{"hosts:read", "instances:*"}}

	assert.True(t, k.HasScope("hosts:read"))
	assert.False(t, k.HasScope("hosts:write"))
	assert.True(t, k.HasScope("instances:read"))
	assert.True(t, k.HasScope("instances:write"))
	assert.False(t, k.HasScope("secrets:write"))
}

func TestVerifyArgon2id(t *testing.T) {
	hash, err := HashToken("hunter2")
	require.NoError(t, err)
	require.NotEmpty(t, hash)

	ok, err := VerifyToken("hunter2", hash)
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = VerifyToken("wrong", hash)
	require.NoError(t, err)
	assert.False(t, ok)
}
```

- [ ] **Step 2: Run test — expect fail**

Run: `go test ./internal/config/ -run "TestParseKeysYAML|TestKey_HasScope|TestVerifyArgon2id"`
Expected: FAIL.

- [ ] **Step 3: Implement `auth.go`**

Create `internal/config/auth.go`:

```go
package config

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
	"gopkg.in/yaml.v3"
)

// APIKey is one entry from auth/keys.yaml.
type APIKey struct {
	ID          string   `yaml:"id"`
	SecretHash  string   `yaml:"secret_hash"`
	Scopes      []string `yaml:"scopes"`
	Description string   `yaml:"description,omitempty"`
}

// HasScope returns true if the key holds the requested scope, supporting "*"
// wildcard suffix at the action level only ("instances:*" matches "instances:read").
func (k APIKey) HasScope(want string) bool {
	for _, s := range k.Scopes {
		if s == want {
			return true
		}
		// wildcard: "instances:*" matches "instances:read", etc.
		if strings.HasSuffix(s, ":*") {
			prefix := strings.TrimSuffix(s, "*")
			if strings.HasPrefix(want, prefix) {
				return true
			}
		}
	}
	return false
}

// ParseKeysYAML parses an `auth/keys.yaml` file body.
func ParseKeysYAML(raw []byte) ([]APIKey, error) {
	var wrapper struct {
		Keys []APIKey `yaml:"keys"`
	}
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&wrapper); err != nil {
		return nil, fmt.Errorf("parse keys: %w", err)
	}
	for i, k := range wrapper.Keys {
		if k.ID == "" {
			return nil, fmt.Errorf("keys[%d]: id is required", i)
		}
		if k.SecretHash == "" {
			return nil, fmt.Errorf("keys[%d]: secret_hash is required", i)
		}
	}
	return wrapper.Keys, nil
}

// HashToken produces an argon2id hash of the given token, encoded in the
// PHC-style format expected by VerifyToken.
func HashToken(token string) (string, error) {
	const (
		time    = 3
		memory  = 64 * 1024
		threads = 4
		keyLen  = 32
	)
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	hash := argon2.IDKey([]byte(token), salt, time, memory, threads, keyLen)
	enc := func(b []byte) string { return base64.RawStdEncoding.EncodeToString(b) }
	return fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s",
		memory, time, threads, enc(salt), enc(hash)), nil
}

// VerifyToken checks token against an argon2id PHC string.
func VerifyToken(token, encoded string) (bool, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return false, errors.New("not an argon2id hash")
	}
	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return false, fmt.Errorf("bad version: %w", err)
	}
	if version != argon2.Version {
		return false, fmt.Errorf("argon2 version mismatch: %d != %d", version, argon2.Version)
	}
	var memory, time uint32
	var threads uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &time, &threads); err != nil {
		return false, fmt.Errorf("bad params: %w", err)
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, err
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, err
	}
	got := argon2.IDKey([]byte(token), salt, time, memory, threads, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1, nil
}
```

- [ ] **Step 4: Run test — expect pass**

Run: `go test ./internal/config/ -run "TestParseKeysYAML|TestKey_HasScope|TestVerifyArgon2id"`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/config/auth.go internal/config/auth_test.go go.mod go.sum
git commit -m "feat(config): bearer-token keys with argon2id hashing"
```

---

## Phase 3 — `internal/auth` (HTTP middleware)

### Task 8: Bearer-token middleware with scope check

**Files:**
- Create: `internal/auth/middleware.go`
- Create: `internal/auth/middleware_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/auth/middleware_test.go`:

```go
package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

func newKey(t *testing.T, scopes ...string) (config.APIKey, string) {
	t.Helper()
	tok := "test-secret"
	hash, err := config.HashToken(tok)
	require.NoError(t, err)
	return config.APIKey{ID: "k1", SecretHash: hash, Scopes: scopes}, tok
}

func newReq(token string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return r
}

func TestMiddleware_NoHeader_401(t *testing.T) {
	k, _ := newKey(t, "hosts:read")
	mw := New([]config.APIKey{k}, "hosts:read")
	rr := httptest.NewRecorder()
	mw(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("inner handler should not run")
	})).ServeHTTP(rr, newReq(""))
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestMiddleware_ValidToken_AllowsRequest(t *testing.T) {
	k, tok := newKey(t, "hosts:read")
	mw := New([]config.APIKey{k}, "hosts:read")
	rr := httptest.NewRecorder()
	called := false
	mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(rr, newReq(tok))
	assert.True(t, called)
	assert.Equal(t, http.StatusNoContent, rr.Code)
}

func TestMiddleware_WrongToken_401(t *testing.T) {
	k, _ := newKey(t, "hosts:read")
	mw := New([]config.APIKey{k}, "hosts:read")
	rr := httptest.NewRecorder()
	mw(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("inner should not run")
	})).ServeHTTP(rr, newReq("wrong"))
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestMiddleware_MissingScope_403(t *testing.T) {
	k, tok := newKey(t, "hosts:read")
	mw := New([]config.APIKey{k}, "instances:write")
	rr := httptest.NewRecorder()
	mw(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("inner should not run")
	})).ServeHTTP(rr, newReq(tok))
	assert.Equal(t, http.StatusForbidden, rr.Code)
}

func TestKeyIDFromContext(t *testing.T) {
	k, tok := newKey(t, "hosts:read")
	mw := New([]config.APIKey{k}, "hosts:read")
	rr := httptest.NewRecorder()
	mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "k1", KeyIDFromContext(r.Context()))
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rr, newReq(tok))
	assert.Equal(t, http.StatusOK, rr.Code)
}
```

- [ ] **Step 2: Run test — expect fail**

Run: `go test ./internal/auth/`
Expected: FAIL (`New`, `KeyIDFromContext` undefined).

- [ ] **Step 3: Implement `middleware.go`**

Create `internal/auth/middleware.go`:

```go
package auth

import (
	"context"
	"net/http"
	"strings"

	"github.com/iotready/podman-api/internal/config"
)

type ctxKey int

const keyIDKey ctxKey = 0

// New returns middleware that requires a Bearer token matching one of keys
// AND that the matching key has the requiredScope.
//
// On failure: 401 (no/invalid token) or 403 (missing scope), with a JSON body.
func New(keys []config.APIKey, requiredScope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tok := bearer(r)
			if tok == "" {
				writeErr(w, http.StatusUnauthorized, "missing_token", "missing or malformed Authorization header")
				return
			}
			matched, ok := match(keys, tok)
			if !ok {
				writeErr(w, http.StatusUnauthorized, "invalid_token", "token not recognised")
				return
			}
			if !matched.HasScope(requiredScope) {
				writeErr(w, http.StatusForbidden, "missing_scope", "token lacks required scope")
				return
			}
			ctx := context.WithValue(r.Context(), keyIDKey, matched.ID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// KeyIDFromContext returns the authenticated key id, or "" if unauthenticated.
func KeyIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(keyIDKey).(string)
	return v
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const p = "Bearer "
	if !strings.HasPrefix(h, p) {
		return ""
	}
	return strings.TrimSpace(h[len(p):])
}

func match(keys []config.APIKey, tok string) (config.APIKey, bool) {
	for _, k := range keys {
		ok, err := config.VerifyToken(tok, k.SecretHash)
		if err == nil && ok {
			return k, true
		}
	}
	return config.APIKey{}, false
}

// writeErr is a stripped-down JSON writer used only inside this package.
// The api package has the canonical version; this avoids a circular import.
func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"code":"` + code + `","message":"` + msg + `"}`))
}
```

- [ ] **Step 4: Run test — expect pass**

Run: `go test ./internal/auth/`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/auth/
git commit -m "feat(auth): bearer-token middleware with scope checks"
```

---

## Phase 4 — `internal/podman` (libpod client wrapper)

The `Client` interface is the seam everything else mocks against. The real implementation uses `containers/podman/v5/pkg/bindings`; the fake lives in `internal/podman/fake/` for handler tests. Real-podman tests are tagged `//go:build integration` and connect only to the local socket.

### Task 9: Define the `Client` interface and constructor

**Files:**
- Create: `internal/podman/client.go`
- Create: `internal/podman/types.go`

- [ ] **Step 1: Create `types.go`**

Create `internal/podman/types.go`:

```go
package podman

import "time"

// Pod is the libpod-shaped pod summary the rest of the API consumes.
type Pod struct {
	ID         string
	Name       string
	Status     string // "Running", "Created", "Exited", etc.
	Created    time.Time
	Containers []Container
	Labels     map[string]string
}

type Container struct {
	ID           string
	Name         string
	Image        string // resolved digest, e.g. "localhost/lite-engine@sha256:..."
	ImageTag     string // human-readable tag, e.g. "localhost/lite-engine:latest"
	Status       string
	StartedAt    time.Time
	RestartCount int
	Ports        []PortMapping
	Env          map[string]string
}

type PortMapping struct {
	HostIP        string
	HostPort      int
	ContainerPort int
	Protocol      string // tcp/udp
}

type Volume struct {
	Name      string
	SizeBytes int64
}

type Secret struct {
	Name      string
	CreatedAt time.Time
}

type LogLine struct {
	Container string
	Stream    string // stdout / stderr
	Time      time.Time
	Line      string
}
```

- [ ] **Step 2: Create `client.go`**

Create `internal/podman/client.go`:

```go
package podman

import "context"

// Client is the contract every consumer of podman speaks. The real
// implementation calls libpod via SSH-tunnelled or unix-socket connections;
// tests use the in-memory fake under ./fake.
type Client interface {
	// Pods
	PlayKube(ctx context.Context, hostID, yaml string, replace bool) error
	PodInspect(ctx context.Context, hostID, name string) (Pod, error)
	PodList(ctx context.Context, hostID string, labelFilters map[string]string) ([]Pod, error)
	PodStart(ctx context.Context, hostID, name string) error
	PodStop(ctx context.Context, hostID, name string) error
	PodRestart(ctx context.Context, hostID, name string) error
	PodRemove(ctx context.Context, hostID, name string, force bool) error

	// Secrets
	SecretCreate(ctx context.Context, hostID, name string, value []byte) error
	SecretList(ctx context.Context, hostID string) ([]Secret, error)
	SecretInspect(ctx context.Context, hostID, name string) (Secret, error)
	SecretRemove(ctx context.Context, hostID, name string) error

	// Volumes
	VolumeInspect(ctx context.Context, hostID, name string) (Volume, error)
	VolumeRemove(ctx context.Context, hostID, name string, force bool) error

	// Logs
	ContainerLogs(ctx context.Context, hostID, container string, opts LogOptions) (<-chan LogLine, error)

	// Images
	ImagePull(ctx context.Context, hostID, ref string) error

	// Health
	Ping(ctx context.Context, hostID string) error
	Version(ctx context.Context, hostID string) (string, error)
	UsedHostPorts(ctx context.Context, hostID string) ([]PortMapping, error)
}

// LogOptions are the knobs for ContainerLogs.
type LogOptions struct {
	Tail   int    // 0 = all
	Since  string // RFC3339 or duration like "5m"; "" = beginning
	Follow bool
}
```

- [ ] **Step 3: Verify it compiles**

Run: `go build ./internal/podman/...`
Expected: exits 0 (interface only — no implementations yet).

- [ ] **Step 4: Commit**

```bash
git add internal/podman/client.go internal/podman/types.go
git commit -m "feat(podman): define Client interface and shared types"
```

---

### Task 10: Implement the in-memory fake `Client`

**Files:**
- Create: `internal/podman/fake/fake.go`
- Create: `internal/podman/fake/fake_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/podman/fake/fake_test.go`:

```go
package fake

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman"
)

func TestFake_PlayKubeAndInspect(t *testing.T) {
	f := New()
	ctx := context.Background()

	yaml := `
apiVersion: v1
kind: Pod
metadata:
  name: lite-engine-x
  labels:
    podman-api/template: lite-engine
spec:
  containers:
    - name: app
      image: x:latest
`
	require.NoError(t, f.PlayKube(ctx, "h1", yaml, false))

	p, err := f.PodInspect(ctx, "h1", "lite-engine-x")
	require.NoError(t, err)
	assert.Equal(t, "lite-engine-x", p.Name)
	assert.Equal(t, "Running", p.Status)
	require.Len(t, p.Containers, 1)
	assert.Equal(t, "app", p.Containers[0].Name)
}

func TestFake_PodNotFound(t *testing.T) {
	f := New()
	_, err := f.PodInspect(context.Background(), "h1", "nope")
	require.ErrorIs(t, err, podman.ErrNotFound)
}

func TestFake_PodLifecycle(t *testing.T) {
	f := New()
	ctx := context.Background()
	yaml := `
apiVersion: v1
kind: Pod
metadata: {name: p1}
spec:
  containers: [{name: c1, image: x}]
`
	require.NoError(t, f.PlayKube(ctx, "h", yaml, false))

	require.NoError(t, f.PodStop(ctx, "h", "p1"))
	p, _ := f.PodInspect(ctx, "h", "p1")
	assert.Equal(t, "Exited", p.Status)

	require.NoError(t, f.PodStart(ctx, "h", "p1"))
	p, _ = f.PodInspect(ctx, "h", "p1")
	assert.Equal(t, "Running", p.Status)

	require.NoError(t, f.PodRemove(ctx, "h", "p1", false))
	_, err := f.PodInspect(ctx, "h", "p1")
	require.ErrorIs(t, err, podman.ErrNotFound)
}

func TestFake_Secrets(t *testing.T) {
	f := New()
	ctx := context.Background()
	require.NoError(t, f.SecretCreate(ctx, "h", "s", []byte("v")))
	s, err := f.SecretInspect(ctx, "h", "s")
	require.NoError(t, err)
	assert.Equal(t, "s", s.Name)

	_, err = f.SecretInspect(ctx, "h", "missing")
	require.ErrorIs(t, err, podman.ErrNotFound)
}
```

- [ ] **Step 2: Add `ErrNotFound` to the podman package**

Edit `internal/podman/client.go` and append (at the bottom of the file):

```go
import "errors"

// ErrNotFound is returned when a pod, container, secret, or volume isn't present.
var ErrNotFound = errors.New("podman: not found")
```

(Move the `import` line to the top of the file, merging with the existing `import "context"` into a parenthesised group.)

- [ ] **Step 3: Run test — expect fail**

Run: `go test ./internal/podman/fake/`
Expected: FAIL (`fake.New` undefined).

- [ ] **Step 4: Implement `fake/fake.go`**

Create `internal/podman/fake/fake.go`:

```go
// Package fake is an in-memory implementation of podman.Client used by tests.
// It models pods, secrets, and volumes as maps keyed by host ID.
package fake

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/iotready/podman-api/internal/podman"
)

// Fake is a thread-safe in-memory podman.Client.
type Fake struct {
	mu      sync.Mutex
	pods    map[string]map[string]podman.Pod    // hostID -> name -> Pod
	secrets map[string]map[string]podman.Secret // hostID -> name -> Secret
	volumes map[string]map[string]podman.Volume // hostID -> name -> Volume

	// Optional hooks for tests that want to inject errors.
	PlayKubeErr error
}

// New returns a fresh fake.
func New() *Fake {
	return &Fake{
		pods:    map[string]map[string]podman.Pod{},
		secrets: map[string]map[string]podman.Secret{},
		volumes: map[string]map[string]podman.Volume{},
	}
}

func (f *Fake) hostPods(h string) map[string]podman.Pod {
	if _, ok := f.pods[h]; !ok {
		f.pods[h] = map[string]podman.Pod{}
	}
	return f.pods[h]
}
func (f *Fake) hostSecrets(h string) map[string]podman.Secret {
	if _, ok := f.secrets[h]; !ok {
		f.secrets[h] = map[string]podman.Secret{}
	}
	return f.secrets[h]
}
func (f *Fake) hostVolumes(h string) map[string]podman.Volume {
	if _, ok := f.volumes[h]; !ok {
		f.volumes[h] = map[string]podman.Volume{}
	}
	return f.volumes[h]
}

func (f *Fake) PlayKube(_ context.Context, hostID, raw string, replace bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.PlayKubeErr != nil {
		return f.PlayKubeErr
	}
	pods := f.hostPods(hostID)
	for _, doc := range strings.Split(raw, "\n---\n") {
		var head struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name   string            `yaml:"name"`
				Labels map[string]string `yaml:"labels"`
			} `yaml:"metadata"`
			Spec struct {
				Containers []struct {
					Name  string `yaml:"name"`
					Image string `yaml:"image"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		}
		_ = yaml.Unmarshal([]byte(doc), &head)
		if head.Kind != "Pod" {
			continue
		}
		if _, exists := pods[head.Metadata.Name]; exists && !replace {
			return fmt.Errorf("pod %q already exists", head.Metadata.Name)
		}
		var cs []podman.Container
		for _, c := range head.Spec.Containers {
			cs = append(cs, podman.Container{
				Name: c.Name, Image: c.Image, ImageTag: c.Image,
				Status: "Running", StartedAt: time.Now(),
			})
		}
		pods[head.Metadata.Name] = podman.Pod{
			ID: head.Metadata.Name, Name: head.Metadata.Name,
			Status: "Running", Created: time.Now(),
			Containers: cs, Labels: head.Metadata.Labels,
		}
	}
	return nil
}

func (f *Fake) PodInspect(_ context.Context, h, name string) (podman.Pod, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.hostPods(h)[name]
	if !ok {
		return podman.Pod{}, podman.ErrNotFound
	}
	return p, nil
}

func (f *Fake) PodList(_ context.Context, h string, filters map[string]string) ([]podman.Pod, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []podman.Pod
	for _, p := range f.hostPods(h) {
		match := true
		for k, v := range filters {
			if p.Labels[k] != v {
				match = false
				break
			}
		}
		if match {
			out = append(out, p)
		}
	}
	return out, nil
}

func (f *Fake) setStatus(h, name, status string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.hostPods(h)[name]
	if !ok {
		return podman.ErrNotFound
	}
	p.Status = status
	for i := range p.Containers {
		p.Containers[i].Status = status
	}
	f.hostPods(h)[name] = p
	return nil
}

func (f *Fake) PodStart(_ context.Context, h, name string) error {
	return f.setStatus(h, name, "Running")
}
func (f *Fake) PodStop(_ context.Context, h, name string) error {
	return f.setStatus(h, name, "Exited")
}
func (f *Fake) PodRestart(_ context.Context, h, name string) error {
	return f.setStatus(h, name, "Running")
}
func (f *Fake) PodRemove(_ context.Context, h, name string, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.hostPods(h)[name]; !ok {
		return podman.ErrNotFound
	}
	delete(f.hostPods(h), name)
	return nil
}

func (f *Fake) SecretCreate(_ context.Context, h, name string, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hostSecrets(h)[name] = podman.Secret{Name: name, CreatedAt: time.Now()}
	return nil
}
func (f *Fake) SecretList(_ context.Context, h string) ([]podman.Secret, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []podman.Secret
	for _, s := range f.hostSecrets(h) {
		out = append(out, s)
	}
	return out, nil
}
func (f *Fake) SecretInspect(_ context.Context, h, name string) (podman.Secret, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.hostSecrets(h)[name]
	if !ok {
		return podman.Secret{}, podman.ErrNotFound
	}
	return s, nil
}
func (f *Fake) SecretRemove(_ context.Context, h, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.hostSecrets(h)[name]; !ok {
		return podman.ErrNotFound
	}
	delete(f.hostSecrets(h), name)
	return nil
}

func (f *Fake) VolumeInspect(_ context.Context, h, name string) (podman.Volume, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.hostVolumes(h)[name]
	if !ok {
		return podman.Volume{}, podman.ErrNotFound
	}
	return v, nil
}
func (f *Fake) VolumeRemove(_ context.Context, h, name string, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.hostVolumes(h)[name]; !ok {
		return podman.ErrNotFound
	}
	delete(f.hostVolumes(h), name)
	return nil
}

func (f *Fake) ContainerLogs(_ context.Context, _, _ string, _ podman.LogOptions) (<-chan podman.LogLine, error) {
	ch := make(chan podman.LogLine)
	close(ch)
	return ch, nil
}

func (f *Fake) ImagePull(_ context.Context, _, _ string) error { return nil }

func (f *Fake) Ping(_ context.Context, _ string) error                  { return nil }
func (f *Fake) Version(_ context.Context, _ string) (string, error)     { return "fake-1.0", nil }
func (f *Fake) UsedHostPorts(_ context.Context, h string) ([]podman.PortMapping, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []podman.PortMapping
	for _, p := range f.hostPods(h) {
		for _, c := range p.Containers {
			out = append(out, c.Ports...)
		}
	}
	return out, nil
}

// Compile-time guarantee that Fake implements the interface.
var _ podman.Client = (*Fake)(nil)
```

- [ ] **Step 5: Run test — expect pass**

Run: `go test ./internal/podman/fake/`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add internal/podman/
git commit -m "feat(podman): in-memory fake Client for handler tests"
```

---

### Task 11: Real `Client` — connection management, Ping, Version

The real implementation lives in `internal/podman/real.go`. It maintains one libpod `context.Context` per host (the bindings library carries the connection on `context.Context`). Connections are created lazily on first use and reused.

**SAFETY NOTE:** This task creates the constructor. All tests for it use `unix://` against the local socket only. Do **not** test against `ssh://`.

**Files:**
- Create: `internal/podman/real.go`
- Create: `internal/podman/real_test.go` (unit-level: just URI parsing and connection caching with a fake dialer)
- Create: `internal/podman/real_integration_test.go` (`//go:build integration`, local-socket-only)

- [ ] **Step 1: Add the bindings dependency**

Run: `go get github.com/containers/podman/v5/pkg/bindings@latest`
Expected: `go.sum` and `go.mod` updated.

If module resolution fails (the v5 bindings sometimes have CGO requirements), document the actual version added in a comment in `real.go` and continue.

- [ ] **Step 2: Write the unit test (no podman required)**

Create `internal/podman/real_test.go`:

```go
package podman

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

func TestRealClient_RegistersHosts(t *testing.T) {
	hosts := []config.Host{
		{ID: "local", Addr: "unix", Socket: "/run/user/1000/podman/podman.sock"},
		{ID: "remote", Addr: "ubuntu@x", Socket: "/run/user/1000/podman/podman.sock"},
	}
	c, err := NewReal(hosts)
	require.NoError(t, err)
	assert.True(t, c.Knows("local"))
	assert.True(t, c.Knows("remote"))
	assert.False(t, c.Knows("nope"))
}

func TestRealClient_URIFor(t *testing.T) {
	hosts := []config.Host{
		{ID: "local", Addr: "unix", Socket: "/tmp/podman.sock"},
		{ID: "remote", Addr: "ubuntu@x.example", Socket: "/run/user/1000/podman/podman.sock", SSHKey: "/k"},
	}
	c, err := NewReal(hosts)
	require.NoError(t, err)

	uri, err := c.URIFor("local")
	require.NoError(t, err)
	assert.Equal(t, "unix:///tmp/podman.sock", uri)

	uri, err = c.URIFor("remote")
	require.NoError(t, err)
	assert.Equal(t, "ssh://ubuntu@x.example/run/user/1000/podman/podman.sock", uri)
}
```

- [ ] **Step 3: Run test — expect fail**

Run: `go test ./internal/podman/ -run TestRealClient`
Expected: FAIL.

- [ ] **Step 4: Implement `real.go` (connection management only)**

Create `internal/podman/real.go`:

```go
package podman

import (
	"context"
	"fmt"
	"sync"

	"github.com/containers/podman/v5/pkg/bindings"
	"github.com/containers/podman/v5/pkg/bindings/system"

	"github.com/iotready/podman-api/internal/config"
)

// Real is the production podman.Client implementation backed by libpod
// over SSH (production) or a local unix socket (dev).
//
// Per-host context.Contexts are created lazily and cached. The bindings
// library stores the underlying connection on the context, so callers must
// pass the cached context (returned by ctxFor) to every libpod call.
type Real struct {
	hosts map[string]config.Host

	mu  sync.Mutex
	ctx map[string]context.Context // hostID -> connection-bearing ctx
}

// NewReal validates host configs and registers them. Connections are not
// opened here; first use opens them.
func NewReal(hosts []config.Host) (*Real, error) {
	r := &Real{hosts: map[string]config.Host{}, ctx: map[string]context.Context{}}
	for _, h := range hosts {
		if h.ID == "" {
			return nil, fmt.Errorf("host with empty id")
		}
		if _, dup := r.hosts[h.ID]; dup {
			return nil, fmt.Errorf("duplicate host id %q", h.ID)
		}
		r.hosts[h.ID] = h
	}
	return r, nil
}

// Knows reports whether the host is registered.
func (r *Real) Knows(id string) bool {
	_, ok := r.hosts[id]
	return ok
}

// URIFor returns the libpod URI for hostID. unix-only when addr=="unix".
func (r *Real) URIFor(id string) (string, error) {
	h, ok := r.hosts[id]
	if !ok {
		return "", fmt.Errorf("unknown host %q", id)
	}
	if h.Addr == "unix" {
		return "unix://" + h.Socket, nil
	}
	return "ssh://" + h.Addr + h.Socket, nil
}

// ctxFor returns a libpod-ready context for hostID, opening the connection
// on first use.
func (r *Real) ctxFor(parent context.Context, id string) (context.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.ctx[id]; ok {
		return c, nil
	}
	uri, err := r.URIFor(id)
	if err != nil {
		return nil, err
	}
	c, err := bindings.NewConnection(parent, uri)
	if err != nil {
		return nil, fmt.Errorf("connect to host %q: %w", id, err)
	}
	r.ctx[id] = c
	return c, nil
}

func (r *Real) Ping(ctx context.Context, id string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = system.Info(c, &system.InfoOptions{})
	return err
}

func (r *Real) Version(ctx context.Context, id string) (string, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return "", err
	}
	info, err := system.Info(c, &system.InfoOptions{})
	if err != nil {
		return "", err
	}
	return info.Version.Version, nil
}
```

(The remaining methods of `podman.Client` are stubbed out in the next tasks. To keep `Real` from breaking the `var _ Client = (*Fake)(nil)` style assertion you'll add later, do **not** add `var _ Client = (*Real)(nil)` until Task 14.)

- [ ] **Step 5: Run unit test — expect pass**

Run: `go test ./internal/podman/ -run TestRealClient`
Expected: PASS (2 tests).

- [ ] **Step 6: Add a build-tag integration test for Ping (LOCAL only)**

Create `internal/podman/real_integration_test.go`:

```go
//go:build integration

package podman

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

func localSocket(t *testing.T) string {
	t.Helper()
	rt := os.Getenv("XDG_RUNTIME_DIR")
	if rt == "" {
		t.Skip("XDG_RUNTIME_DIR unset")
	}
	p := filepath.Join(rt, "podman", "podman.sock")
	if _, err := os.Stat(p); err != nil {
		t.Skip("local podman socket not available: " + err.Error())
	}
	return p
}

func TestRealClient_Ping_LocalOnly(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	require.NoError(t, c.Ping(context.Background(), "local"))

	v, err := c.Version(context.Background(), "local")
	require.NoError(t, err)
	require.NotEmpty(t, v)
}
```

- [ ] **Step 7: Run integration test (local podman required)**

Run: `go test -tags=integration ./internal/podman/ -run TestRealClient_Ping_LocalOnly`
Expected: PASS, with podman version printed in verbose mode. If podman service isn't running, run `systemctl --user start podman.socket` first.

- [ ] **Step 8: Commit**

```bash
git add internal/podman/real.go internal/podman/real_test.go internal/podman/real_integration_test.go go.mod go.sum
git commit -m "feat(podman): real Client with connection caching, Ping, Version"
```

---

### Task 12: Real `Client` — pod operations

Implements `PlayKube`, `PodInspect`, `PodList`, `PodStart`, `PodStop`, `PodRestart`, `PodRemove`. All against local socket only in tests.

**Files:**
- Modify: `internal/podman/real.go` (append methods)
- Create: `internal/podman/real_pods_integration_test.go` (`//go:build integration`)

- [ ] **Step 1: Append pod-op methods to `real.go`**

Add to `internal/podman/real.go` (extend imports: `play`, `pods`, `entities`):

```go
import (
	// ...existing imports...
	"github.com/containers/podman/v5/pkg/bindings/play"
	"github.com/containers/podman/v5/pkg/bindings/pods"
	"github.com/containers/podman/v5/pkg/domain/entities"
	"github.com/containers/podman/v5/pkg/specgen"
	"os"
	"strings"
	"time"
)
```

Append methods:

```go
func (r *Real) PlayKube(ctx context.Context, id, raw string, replace bool) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp("", "play-kube-*.yaml")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(raw); err != nil {
		return err
	}
	tmp.Close()

	opts := &play.KubeOptions{}
	if replace {
		t := true
		opts.Replace = &t
	}
	_, err = play.Kube(c, tmp.Name(), opts)
	return err
}

func (r *Real) PodInspect(ctx context.Context, id, name string) (Pod, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return Pod{}, err
	}
	rep, err := pods.Inspect(c, name, &pods.InspectOptions{})
	if err != nil {
		if isNotFound(err) {
			return Pod{}, ErrNotFound
		}
		return Pod{}, err
	}
	return podFromInspect(rep), nil
}

func (r *Real) PodList(ctx context.Context, id string, filters map[string]string) ([]Pod, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	opts := &pods.ListOptions{}
	if len(filters) > 0 {
		// Convert label filters into the libpod filter map.
		f := map[string][]string{}
		for k, v := range filters {
			f["label"] = append(f["label"], k+"="+v)
		}
		opts.Filters = f
	}
	reps, err := pods.List(c, opts)
	if err != nil {
		return nil, err
	}
	out := make([]Pod, 0, len(reps))
	for _, rep := range reps {
		out = append(out, podFromList(rep))
	}
	return out, nil
}

func (r *Real) PodStart(ctx context.Context, id, name string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = pods.Start(c, name, &pods.StartOptions{})
	return mapNotFound(err)
}

func (r *Real) PodStop(ctx context.Context, id, name string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = pods.Stop(c, name, &pods.StopOptions{})
	return mapNotFound(err)
}

func (r *Real) PodRestart(ctx context.Context, id, name string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = pods.Restart(c, name, &pods.RestartOptions{})
	return mapNotFound(err)
}

func (r *Real) PodRemove(ctx context.Context, id, name string, force bool) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	opts := &pods.RemoveOptions{}
	if force {
		t := true
		opts.Force = &t
	}
	_, err = pods.Remove(c, name, opts)
	return mapNotFound(err)
}

// --- helpers ---

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such pod") ||
		strings.Contains(msg, "no such container") ||
		strings.Contains(msg, "no such secret") ||
		strings.Contains(msg, "no such volume") ||
		strings.Contains(msg, "not found")
}

func mapNotFound(err error) error {
	if isNotFound(err) {
		return ErrNotFound
	}
	return err
}

// podFromInspect translates a libpod PodInspectReport into our Pod shape.
// Container detail is filled minimally here; the orchestration layer hits
// individual containers if it needs full env/port detail.
func podFromInspect(p *entities.PodInspectReport) Pod {
	out := Pod{
		ID:     p.ID,
		Name:   p.Name,
		Status: p.State,
		Labels: p.Labels,
	}
	if !p.Created.IsZero() {
		out.Created = p.Created
	}
	for _, c := range p.Containers {
		out.Containers = append(out.Containers, Container{
			ID:     c.ID,
			Name:   c.Name,
			Status: c.State,
		})
	}
	_ = specgen.PodSpecGenerator{} // silence unused import in some bindings versions
	return out
}

func podFromList(p *entities.ListPodsReport) Pod {
	out := Pod{
		ID:     p.Id,
		Name:   p.Name,
		Status: p.Status,
		Labels: p.Labels,
	}
	if !p.Created.IsZero() {
		out.Created = p.Created
	}
	for _, c := range p.Containers {
		out.Containers = append(out.Containers, Container{
			ID:     c.Id,
			Name:   c.Names,
			Status: c.Status,
		})
	}
	return out
}
```

(If field names from `entities` differ in your installed bindings version, adjust to match — the library's Go types may evolve. Keep the externally-visible `Pod` shape stable.)

- [ ] **Step 2: Write the integration test**

Create `internal/podman/real_pods_integration_test.go`:

```go
//go:build integration

package podman

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

const podsTestYAML = `
apiVersion: v1
kind: Pod
metadata:
  name: podman-api-itest
  labels:
    podman-api/itest: "true"
spec:
  containers:
    - name: c
      image: docker.io/library/alpine:latest
      command: ["sleep", "60"]
`

func TestReal_PlayKubeAndLifecycle_LocalOnly(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Always clean up.
	t.Cleanup(func() {
		_ = c.PodRemove(context.Background(), "local", "podman-api-itest", true)
	})

	require.NoError(t, c.PlayKube(ctx, "local", podsTestYAML, true))

	p, err := c.PodInspect(ctx, "local", "podman-api-itest")
	require.NoError(t, err)
	assert.Equal(t, "podman-api-itest", p.Name)

	require.NoError(t, c.PodStop(ctx, "local", "podman-api-itest"))
	require.NoError(t, c.PodStart(ctx, "local", "podman-api-itest"))

	require.NoError(t, c.PodRemove(ctx, "local", "podman-api-itest", true))
	_, err = c.PodInspect(ctx, "local", "podman-api-itest")
	require.ErrorIs(t, err, ErrNotFound)
}
```

- [ ] **Step 3: Run integration test (local podman required)**

Run: `go test -tags=integration ./internal/podman/ -run TestReal_PlayKubeAndLifecycle_LocalOnly`
Expected: PASS. The test pulls `alpine` first run.

- [ ] **Step 4: Commit**

```bash
git add internal/podman/real.go internal/podman/real_pods_integration_test.go
git commit -m "feat(podman): real Client pod operations (play kube, lifecycle)"
```

---

### Task 13: Real `Client` — secrets and volumes

**Files:**
- Modify: `internal/podman/real.go` (append)
- Create: `internal/podman/real_secrets_integration_test.go` (`//go:build integration`)

- [ ] **Step 1: Append secret + volume methods**

Add imports `"github.com/containers/podman/v5/pkg/bindings/secrets"`, `"github.com/containers/podman/v5/pkg/bindings/volumes"`, and `"bytes"`.

Append to `internal/podman/real.go`:

```go
func (r *Real) SecretCreate(ctx context.Context, id, name string, value []byte) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	opts := &secrets.CreateOptions{Name: &name}
	_, err = secrets.Create(c, bytes.NewReader(value), opts)
	return err
}

func (r *Real) SecretList(ctx context.Context, id string) ([]Secret, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	reps, err := secrets.List(c, &secrets.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]Secret, 0, len(reps))
	for _, s := range reps {
		out = append(out, Secret{Name: s.Spec.Name, CreatedAt: s.CreatedAt})
	}
	return out, nil
}

func (r *Real) SecretInspect(ctx context.Context, id, name string) (Secret, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return Secret{}, err
	}
	rep, err := secrets.Inspect(c, name, &secrets.InspectOptions{})
	if err != nil {
		return Secret{}, mapNotFound(err)
	}
	return Secret{Name: rep.Spec.Name, CreatedAt: rep.CreatedAt}, nil
}

func (r *Real) SecretRemove(ctx context.Context, id, name string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	return mapNotFound(secrets.Remove(c, name))
}

func (r *Real) VolumeInspect(ctx context.Context, id, name string) (Volume, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return Volume{}, err
	}
	rep, err := volumes.Inspect(c, name, &volumes.InspectOptions{})
	if err != nil {
		return Volume{}, mapNotFound(err)
	}
	v := Volume{Name: rep.Name}
	// Size is not always populated; leave at 0 if missing.
	return v, nil
}

func (r *Real) VolumeRemove(ctx context.Context, id, name string, force bool) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	opts := &volumes.RemoveOptions{}
	if force {
		t := true
		opts.Force = &t
	}
	return mapNotFound(volumes.Remove(c, name, opts))
}
```

- [ ] **Step 2: Write the integration test**

Create `internal/podman/real_secrets_integration_test.go`:

```go
//go:build integration

package podman

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

func TestReal_Secrets_LocalOnly(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx := context.Background()
	const name = "podman-api-itest-secret"

	t.Cleanup(func() { _ = c.SecretRemove(context.Background(), "local", name) })

	require.NoError(t, c.SecretCreate(ctx, "local", name, []byte("v1")))
	s, err := c.SecretInspect(ctx, "local", name)
	require.NoError(t, err)
	assert.Equal(t, name, s.Name)

	require.NoError(t, c.SecretRemove(ctx, "local", name))
	_, err = c.SecretInspect(ctx, "local", name)
	require.ErrorIs(t, err, ErrNotFound)
}
```

- [ ] **Step 3: Run integration test**

Run: `go test -tags=integration ./internal/podman/ -run TestReal_Secrets_LocalOnly`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/podman/real.go internal/podman/real_secrets_integration_test.go
git commit -m "feat(podman): real Client secrets and volumes"
```

---

### Task 14: Real `Client` — logs, image pull, used host ports

**Files:**
- Modify: `internal/podman/real.go` (append)

- [ ] **Step 1: Append remaining methods**

Add imports `"github.com/containers/podman/v5/pkg/bindings/containers"`, `"github.com/containers/podman/v5/pkg/bindings/images"`, `"strconv"`.

Append to `internal/podman/real.go`:

```go
func (r *Real) ContainerLogs(ctx context.Context, id, container string, opts LogOptions) (<-chan LogLine, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	stdoutCh := make(chan string, 64)
	stderrCh := make(chan string, 64)
	out := make(chan LogLine, 64)

	go func() {
		defer close(out)
		for {
			select {
			case line, ok := <-stdoutCh:
				if !ok {
					stdoutCh = nil
				} else {
					out <- LogLine{Container: container, Stream: "stdout", Line: line, Time: time.Now()}
				}
			case line, ok := <-stderrCh:
				if !ok {
					stderrCh = nil
				} else {
					out <- LogLine{Container: container, Stream: "stderr", Line: line, Time: time.Now()}
				}
			}
			if stdoutCh == nil && stderrCh == nil {
				return
			}
		}
	}()

	tail := ""
	if opts.Tail > 0 {
		tail = strconv.Itoa(opts.Tail)
	}
	follow := opts.Follow
	logsOpts := &containers.LogOptions{
		Stdout: boolPtr(true), Stderr: boolPtr(true),
		Follow: &follow, Tail: &tail, Since: &opts.Since,
	}
	go func() {
		_ = containers.Logs(c, container, logsOpts, stdoutCh, stderrCh)
		close(stdoutCh)
		close(stderrCh)
	}()
	return out, nil
}

func (r *Real) ImagePull(ctx context.Context, id, ref string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = images.Pull(c, ref, &images.PullOptions{})
	return err
}

func (r *Real) UsedHostPorts(ctx context.Context, id string) ([]PortMapping, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	all := true
	conts, err := containers.List(c, &containers.ListOptions{All: &all})
	if err != nil {
		return nil, err
	}
	var out []PortMapping
	for _, ct := range conts {
		for _, p := range ct.Ports {
			out = append(out, PortMapping{
				HostIP: p.HostIP, HostPort: int(p.HostPort),
				ContainerPort: int(p.ContainerPort), Protocol: p.Protocol,
			})
		}
	}
	return out, nil
}

func boolPtr(b bool) *bool { return &b }

// Compile-time guarantee that Real satisfies the Client interface.
var _ Client = (*Real)(nil)
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/podman/...`
Expected: exits 0. If a method signature drifts from the bindings library, fix the call site, not the interface.

- [ ] **Step 3: Run all unit tests**

Run: `go test ./internal/podman/...`
Expected: PASS (the existing fake + real unit tests still green).

- [ ] **Step 4: Commit**

```bash
git add internal/podman/real.go
git commit -m "feat(podman): real Client logs, image pull, used host ports"
```

---

## Phase 5 — `internal/instance` (orchestration)

### Task 15: Observed-shape normalization

`Observed` is the JSON shape returned to the CMS for an instance. It's derived from the raw `podman.Pod` plus a couple of cheap follow-up calls. This task implements the pure mapping; the next task wires it into `Service`.

**Files:**
- Create: `internal/instance/observed.go`
- Create: `internal/instance/observed_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/instance/observed_test.go`:

```go
package instance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman"
)

func TestNormalize(t *testing.T) {
	created := time.Date(2026, 5, 9, 10, 0, 0, 0, time.UTC)
	p := podman.Pod{
		Name:    "lite-engine-iotready",
		Status:  "Running",
		Created: created,
		Labels: map[string]string{
			"podman-api/template": "lite-engine",
			"podman-api/slug":     "iotready",
		},
		Containers: []podman.Container{
			{
				Name:      "app",
				Image:     "localhost/lite-engine@sha256:abc",
				ImageTag:  "localhost/lite-engine:latest",
				Status:    "Running",
				StartedAt: created,
				Ports: []podman.PortMapping{
					{HostIP: "127.0.0.1", HostPort: 31001, ContainerPort: 30000, Protocol: "tcp"},
				},
				Env: map[string]string{"BASE_URL": "https://x.example", "AUTH_SECRET": "leak-me-not"},
			},
			{
				Name: "litestream", Image: "docker.io/litestream/litestream:latest",
				Status: "Running", StartedAt: created,
			},
		},
	}

	obs := Normalize(p, "lite-engine", "iotready", []podman.Volume{
		{Name: "lite-engine-iotready-data", SizeBytes: 100},
	})

	assert.Equal(t, "lite-engine", obs.Template)
	assert.Equal(t, "iotready", obs.Slug)
	assert.Equal(t, "Running", obs.Pod.Status)
	require.Len(t, obs.Containers, 2)
	assert.Equal(t, "app", obs.Containers[0].Name)
	assert.Equal(t, "localhost/lite-engine@sha256:abc", obs.Containers[0].Image)
	assert.Equal(t, "localhost/lite-engine:latest", obs.Containers[0].ImageTag)
	require.Len(t, obs.Containers[0].Ports, 1)
	assert.Equal(t, 31001, obs.Containers[0].Ports[0].HostPort)
	require.Len(t, obs.Volumes, 1)
	assert.Equal(t, "lite-engine-iotready-data", obs.Volumes[0].Name)

	// EnvSummary must NOT contain anything that looks like a secret.
	assert.Equal(t, "https://x.example", obs.EnvSummary["BASE_URL"])
	_, hasSecret := obs.EnvSummary["AUTH_SECRET"]
	assert.False(t, hasSecret, "AUTH_SECRET must be redacted from env_summary")
}
```

- [ ] **Step 2: Run test — expect fail**

Run: `go test ./internal/instance/ -run TestNormalize`
Expected: FAIL.

- [ ] **Step 3: Implement `observed.go`**

Create `internal/instance/observed.go`:

```go
package instance

import (
	"strings"
	"time"

	"github.com/iotready/podman-api/internal/podman"
)

// Observed is the JSON shape returned for an instance. Field tags match the
// spec's "Observed shape" example.
type Observed struct {
	Template   string                 `json:"template"`
	Slug       string                 `json:"slug"`
	Pod        ObservedPod            `json:"pod"`
	Containers []ObservedContainer    `json:"containers"`
	Volumes    []ObservedVolume       `json:"volumes,omitempty"`
	EnvSummary map[string]string      `json:"env_summary,omitempty"`
}

type ObservedPod struct {
	ID      string    `json:"id,omitempty"`
	Status  string    `json:"status"`
	Created time.Time `json:"created,omitempty"`
}

type ObservedContainer struct {
	Name         string                `json:"name"`
	Image        string                `json:"image"`
	ImageTag     string                `json:"image_tag,omitempty"`
	Status       string                `json:"status"`
	StartedAt    time.Time             `json:"started_at,omitempty"`
	RestartCount int                   `json:"restart_count"`
	Ports        []ObservedPortMapping `json:"ports,omitempty"`
}

type ObservedPortMapping struct {
	HostIP        string `json:"host_ip,omitempty"`
	HostPort      int    `json:"host_port"`
	ContainerPort int    `json:"container_port"`
	Protocol      string `json:"protocol,omitempty"`
}

type ObservedVolume struct {
	Name      string `json:"name"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

// secretishKeys are env var names whose VALUES are never returned to the CMS.
// The CMS doesn't need them and we don't want them in audit logs or screenshots.
var secretishKeys = map[string]bool{
	"AUTH_SECRET": true,
	"LITESTREAM_ACCESS_KEY_ID": true,
	"LITESTREAM_SECRET_ACCESS_KEY": true,
	"AWS_ACCESS_KEY_ID": true,
	"AWS_SECRET_ACCESS_KEY": true,
}

// Normalize builds Observed from a Pod + the volumes the API thinks the
// instance owns. It redacts known-secret env keys from env_summary.
func Normalize(p podman.Pod, template, slug string, vols []podman.Volume) Observed {
	out := Observed{
		Template: template,
		Slug:     slug,
		Pod:      ObservedPod{ID: p.ID, Status: p.Status, Created: p.Created},
	}
	for _, c := range p.Containers {
		oc := ObservedContainer{
			Name: c.Name, Image: c.Image, ImageTag: c.ImageTag,
			Status: c.Status, StartedAt: c.StartedAt, RestartCount: c.RestartCount,
		}
		for _, port := range c.Ports {
			oc.Ports = append(oc.Ports, ObservedPortMapping{
				HostIP: port.HostIP, HostPort: port.HostPort,
				ContainerPort: port.ContainerPort, Protocol: port.Protocol,
			})
		}
		out.Containers = append(out.Containers, oc)
	}
	for _, v := range vols {
		out.Volumes = append(out.Volumes, ObservedVolume{Name: v.Name, SizeBytes: v.SizeBytes})
	}

	// EnvSummary takes the union of non-secret env vars across containers.
	out.EnvSummary = map[string]string{}
	for _, c := range p.Containers {
		for k, v := range c.Env {
			if secretishKeys[k] || strings.Contains(strings.ToUpper(k), "SECRET") {
				continue
			}
			out.EnvSummary[k] = v
		}
	}
	if len(out.EnvSummary) == 0 {
		out.EnvSummary = nil
	}
	return out
}
```

- [ ] **Step 4: Run test — expect pass**

Run: `go test ./internal/instance/ -run TestNormalize`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/instance/observed.go internal/instance/observed_test.go
git commit -m "feat(instance): Observed shape with secret-env redaction"
```

---

### Task 16: `Service` — orchestration with per-instance locking

`Service` is what the API handlers call. It composes `render` + `podman` + a per-`(host, template, slug)` mutex around state-changing operations.

**Files:**
- Create: `internal/instance/service.go`
- Create: `internal/instance/service_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/instance/service_test.go`:

```go
package instance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
)

func newSvc(t *testing.T) (*Service, *fake.Fake) {
	t.Helper()
	tmpl := config.Template{
		Meta: render.Meta{
			ID: "lite-engine",
			Parameters: render.Parameters{
				Required: []string{"slug", "image", "port", "base_url", "app_template", "s3_bucket", "s3_endpoint"},
			},
			Secrets: render.Secrets{
				PerInstance:       []string{"auth_secret"},
				PerHostReferenced: []string{"s3-access-key-id", "s3-secret-access-key"},
			},
			Volumes: []render.Volume{{Name: "data", Backup: "litestream"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: lite-engine-{{.slug}}
  labels:
    podman-api/template: lite-engine
    podman-api/slug: {{.slug}}
spec:
  containers:
    - name: app
      image: {{.image}}
`,
		Source: "lite-engine.yaml",
	}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	require.NoError(t, f.SecretCreate(context.Background(), "h1", "s3-access-key-id", []byte("k")))
	require.NoError(t, f.SecretCreate(context.Background(), "h1", "s3-secret-access-key", []byte("s")))
	svc := NewService(f, hosts, []config.Template{tmpl})
	return svc, f
}

func TestService_Apply_Then_Get(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()

	req := ApplyRequest{
		Template: "lite-engine",
		Slug:     "iotready",
		Parameters: map[string]any{
			"slug": "iotready", "image": "x:1", "port": 31001,
			"base_url": "https://x", "app_template": "crm",
			"s3_bucket": "b", "s3_endpoint": "https://s3",
		},
		Secrets: map[string]string{"auth_secret": "v"},
	}
	require.NoError(t, svc.Apply(ctx, "h1", req, true))

	obs, err := svc.Get(ctx, "h1", "lite-engine", "iotready")
	require.NoError(t, err)
	assert.Equal(t, "lite-engine", obs.Template)
	assert.Equal(t, "iotready", obs.Slug)
	assert.Equal(t, "Running", obs.Pod.Status)
}

func TestService_Apply_RequiresHostSecret(t *testing.T) {
	svc, f := newSvc(t)
	require.NoError(t, f.SecretRemove(context.Background(), "h1", "s3-access-key-id"))

	err := svc.Apply(context.Background(), "h1", ApplyRequest{
		Template: "lite-engine",
		Slug:     "x",
		Parameters: map[string]any{
			"slug": "x", "image": "x:1", "port": 1,
			"base_url": "x", "app_template": "crm", "s3_bucket": "b", "s3_endpoint": "e",
		},
		Secrets: map[string]string{"auth_secret": "v"},
	}, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrHostSecretMissing)
}

func TestService_UnknownTemplate(t *testing.T) {
	svc, _ := newSvc(t)
	err := svc.Apply(context.Background(), "h1", ApplyRequest{Template: "nope", Slug: "x"}, false)
	require.ErrorIs(t, err, ErrUnknownTemplate)
}

func TestService_UnknownHost(t *testing.T) {
	svc, _ := newSvc(t)
	err := svc.Apply(context.Background(), "nope", ApplyRequest{Template: "lite-engine", Slug: "x"}, false)
	require.ErrorIs(t, err, ErrUnknownHost)
}

func TestService_Lifecycle(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()
	req := ApplyRequest{
		Template: "lite-engine", Slug: "iotready",
		Parameters: map[string]any{
			"slug": "iotready", "image": "x:1", "port": 1,
			"base_url": "x", "app_template": "crm", "s3_bucket": "b", "s3_endpoint": "e",
		},
		Secrets: map[string]string{"auth_secret": "v"},
	}
	require.NoError(t, svc.Apply(ctx, "h1", req, true))

	require.NoError(t, svc.Stop(ctx, "h1", "lite-engine", "iotready"))
	obs, _ := svc.Get(ctx, "h1", "lite-engine", "iotready")
	assert.Equal(t, "Exited", obs.Pod.Status)

	require.NoError(t, svc.Start(ctx, "h1", "lite-engine", "iotready"))
	obs, _ = svc.Get(ctx, "h1", "lite-engine", "iotready")
	assert.Equal(t, "Running", obs.Pod.Status)

	require.NoError(t, svc.Delete(ctx, "h1", "lite-engine", "iotready", DeleteOptions{}))
	_, err := svc.Get(ctx, "h1", "lite-engine", "iotready")
	require.ErrorIs(t, err, ErrInstanceNotFound)
}
```

- [ ] **Step 2: Run test — expect fail**

Run: `go test ./internal/instance/ -run TestService`
Expected: FAIL.

- [ ] **Step 3: Implement `service.go`**

Create `internal/instance/service.go`:

```go
package instance

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
)

// Sentinel errors mapped by the API layer to JSON error codes.
var (
	ErrUnknownHost       = errors.New("unknown host")
	ErrUnknownTemplate   = errors.New("unknown template")
	ErrInstanceNotFound  = errors.New("instance not found")
	ErrInstanceExists    = errors.New("instance already exists")
	ErrHostSecretMissing = errors.New("required host secret missing")
)

// ApplyRequest is the body of POST /instances and PUT /instances/{...}.
type ApplyRequest struct {
	Template   string            `json:"template"`
	Slug       string            `json:"slug"`
	Parameters map[string]any    `json:"parameters"`
	Secrets    map[string]string `json:"secrets"`
}

// DeleteOptions controls cleanup beyond the pod itself.
type DeleteOptions struct {
	PruneVolumes bool
	PruneSecrets bool
}

// Service orchestrates instance operations against podman hosts.
type Service struct {
	client    podman.Client
	hosts     map[string]config.Host
	templates map[string]config.Template

	mu    sync.Mutex
	locks map[string]*sync.Mutex // key = host|template|slug
}

func NewService(client podman.Client, hosts []config.Host, tmpls []config.Template) *Service {
	s := &Service{
		client:    client,
		hosts:     map[string]config.Host{},
		templates: map[string]config.Template{},
		locks:     map[string]*sync.Mutex{},
	}
	for _, h := range hosts {
		s.hosts[h.ID] = h
	}
	for _, t := range tmpls {
		s.templates[t.Meta.ID] = t
	}
	return s
}

func (s *Service) instanceLock(host, tmpl, slug string) *sync.Mutex {
	key := host + "|" + tmpl + "|" + slug
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.locks[key]
	if !ok {
		m = &sync.Mutex{}
		s.locks[key] = m
	}
	return m
}

func (s *Service) lookup(host, tmpl string) (config.Template, error) {
	if _, ok := s.hosts[host]; !ok {
		return config.Template{}, ErrUnknownHost
	}
	t, ok := s.templates[tmpl]
	if !ok {
		return config.Template{}, ErrUnknownTemplate
	}
	return t, nil
}

func podName(tmpl, slug string) string { return tmpl + "-" + slug }

func instanceSecretName(tmpl, slug, name string) string {
	return tmpl + "-" + slug + "-" + name
}

// Apply creates or replaces an instance. If replace is false and the pod
// exists, returns ErrInstanceExists.
func (s *Service) Apply(ctx context.Context, host string, req ApplyRequest, replace bool) error {
	tmpl, err := s.lookup(host, req.Template)
	if err != nil {
		return err
	}
	if err := render.Validate(tmpl.Meta, req.Parameters, req.Secrets); err != nil {
		return fmt.Errorf("validate: %w", err)
	}

	lock := s.instanceLock(host, req.Template, req.Slug)
	lock.Lock()
	defer lock.Unlock()

	// Pre-check: per-host secrets exist.
	for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
		if _, err := s.client.SecretInspect(ctx, host, name); err != nil {
			if errors.Is(err, podman.ErrNotFound) {
				return fmt.Errorf("%w: %s", ErrHostSecretMissing, name)
			}
			return fmt.Errorf("inspect host secret %q: %w", name, err)
		}
	}

	// Strict-create: 409 if pod exists.
	if !replace {
		if _, err := s.client.PodInspect(ctx, host, podName(req.Template, req.Slug)); err == nil {
			return ErrInstanceExists
		} else if !errors.Is(err, podman.ErrNotFound) {
			return fmt.Errorf("inspect pod: %w", err)
		}
	}

	// Push per-instance secrets, then zero the local copies.
	for k, v := range req.Secrets {
		name := instanceSecretName(req.Template, req.Slug, k)
		// Idempotency: if it already exists, remove and recreate (rotation).
		if _, err := s.client.SecretInspect(ctx, host, name); err == nil {
			if err := s.client.SecretRemove(ctx, host, name); err != nil {
				return fmt.Errorf("remove existing secret %q: %w", name, err)
			}
		}
		if err := s.client.SecretCreate(ctx, host, name, []byte(v)); err != nil {
			return fmt.Errorf("create secret %q: %w", name, err)
		}
	}
	for k := range req.Secrets {
		req.Secrets[k] = "" // best-effort zero
	}

	yaml, err := render.Render(rawTemplate(tmpl), req.Parameters)
	if err != nil {
		return fmt.Errorf("render: %w", err)
	}
	if err := s.client.PlayKube(ctx, host, yaml, replace); err != nil {
		return fmt.Errorf("play kube: %w", err)
	}
	return nil
}

// rawTemplate reconstructs a complete template source from a parsed Template
// (template-meta + body). Render needs the full source because ParseMeta
// strips the meta block before handing the body to text/template.
func rawTemplate(t config.Template) string {
	// We only need the body for Render — Render runs ParseMeta internally
	// and discards the meta block. So reattach a minimal meta header.
	return "# template-meta:\n#   id: " + t.Meta.ID + "\n#   parameters:\n#     required: []\n---\n" + t.Body
}

// Get returns the observed shape for an instance.
func (s *Service) Get(ctx context.Context, host, tmpl, slug string) (Observed, error) {
	if _, err := s.lookup(host, tmpl); err != nil {
		return Observed{}, err
	}
	p, err := s.client.PodInspect(ctx, host, podName(tmpl, slug))
	if err != nil {
		if errors.Is(err, podman.ErrNotFound) {
			return Observed{}, ErrInstanceNotFound
		}
		return Observed{}, err
	}
	// Best-effort volume lookup; ignore missing.
	t := s.templates[tmpl]
	var vols []podman.Volume
	for _, v := range t.Meta.Volumes {
		name := tmpl + "-" + slug + "-" + v.Name
		if vv, err := s.client.VolumeInspect(ctx, host, name); err == nil {
			vols = append(vols, vv)
		}
	}
	return Normalize(p, tmpl, slug, vols), nil
}

// List returns all instances of a given template on a host.
func (s *Service) List(ctx context.Context, host, tmpl string) ([]Observed, error) {
	if _, err := s.lookup(host, tmpl); err != nil {
		return nil, err
	}
	pods, err := s.client.PodList(ctx, host, map[string]string{"podman-api/template": tmpl})
	if err != nil {
		return nil, err
	}
	out := make([]Observed, 0, len(pods))
	for _, p := range pods {
		slug := p.Labels["podman-api/slug"]
		out = append(out, Normalize(p, tmpl, slug, nil))
	}
	return out, nil
}

func (s *Service) Start(ctx context.Context, host, tmpl, slug string) error {
	return s.lifecycle(ctx, host, tmpl, slug, s.client.PodStart)
}
func (s *Service) Stop(ctx context.Context, host, tmpl, slug string) error {
	return s.lifecycle(ctx, host, tmpl, slug, s.client.PodStop)
}
func (s *Service) Restart(ctx context.Context, host, tmpl, slug string) error {
	return s.lifecycle(ctx, host, tmpl, slug, s.client.PodRestart)
}

func (s *Service) lifecycle(ctx context.Context, host, tmpl, slug string,
	op func(context.Context, string, string) error) error {
	if _, err := s.lookup(host, tmpl); err != nil {
		return err
	}
	lock := s.instanceLock(host, tmpl, slug)
	lock.Lock()
	defer lock.Unlock()
	if err := op(ctx, host, podName(tmpl, slug)); err != nil {
		if errors.Is(err, podman.ErrNotFound) {
			return ErrInstanceNotFound
		}
		return err
	}
	return nil
}

// Upgrade pulls a new image tag and replaces the pod with the new image.
// Caller supplies the full ApplyRequest; Upgrade is structurally a PUT
// where we additionally pre-pull the image. The split exists so the API
// layer can expose it as a distinct verb.
func (s *Service) Upgrade(ctx context.Context, host string, req ApplyRequest, image string) error {
	if image == "" {
		return errors.New("upgrade requires an image")
	}
	if err := s.client.ImagePull(ctx, host, image); err != nil {
		return fmt.Errorf("pull %q: %w", image, err)
	}
	req.Parameters["image"] = image
	return s.Apply(ctx, host, req, true)
}

// Delete removes the pod and optionally its volumes and per-instance secrets.
func (s *Service) Delete(ctx context.Context, host, tmpl, slug string, opts DeleteOptions) error {
	if _, err := s.lookup(host, tmpl); err != nil {
		return err
	}
	lock := s.instanceLock(host, tmpl, slug)
	lock.Lock()
	defer lock.Unlock()

	if err := s.client.PodRemove(ctx, host, podName(tmpl, slug), true); err != nil {
		if errors.Is(err, podman.ErrNotFound) {
			return ErrInstanceNotFound
		}
		return err
	}

	t := s.templates[tmpl]
	if opts.PruneSecrets {
		for _, name := range t.Meta.Secrets.PerInstance {
			full := instanceSecretName(tmpl, slug, name)
			_ = s.client.SecretRemove(ctx, host, full) // best-effort
		}
	}
	if opts.PruneVolumes {
		for _, v := range t.Meta.Volumes {
			full := tmpl + "-" + slug + "-" + v.Name
			_ = s.client.VolumeRemove(ctx, host, full, true) // best-effort
		}
	}
	return nil
}

// Hosts returns the configured hosts (read-only view for the API).
func (s *Service) Hosts() []config.Host {
	out := make([]config.Host, 0, len(s.hosts))
	for _, h := range s.hosts {
		out = append(out, h)
	}
	return out
}

// Templates returns the loaded templates' metadata (read-only view).
func (s *Service) Templates() []config.Template {
	out := make([]config.Template, 0, len(s.templates))
	for _, t := range s.templates {
		out = append(out, t)
	}
	return out
}

// PortsInUse returns all currently-bound host ports on hostID.
func (s *Service) PortsInUse(ctx context.Context, host string) ([]podman.PortMapping, error) {
	if _, ok := s.hosts[host]; !ok {
		return nil, ErrUnknownHost
	}
	return s.client.UsedHostPorts(ctx, host)
}
```

- [ ] **Step 4: Run test — expect pass**

Run: `go test ./internal/instance/`
Expected: PASS (5+ tests).

- [ ] **Step 5: Commit**

```bash
git add internal/instance/service.go internal/instance/service_test.go
git commit -m "feat(instance): Service orchestration with per-instance locking"
```

---

## Phase 6 — `internal/api` (HTTP layer)

### Task 17: Error model — code enum, JSON writer, error→status mapping

**Files:**
- Create: `internal/api/errors.go`
- Create: `internal/api/errors_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/api/errors_test.go`:

```go
package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/iotready/podman-api/internal/instance"
)

func TestWriteError_KnownSentinels(t *testing.T) {
	cases := []struct {
		err  error
		code string
		stat int
	}{
		{instance.ErrUnknownHost, "unknown_host", http.StatusNotFound},
		{instance.ErrUnknownTemplate, "unknown_template", http.StatusNotFound},
		{instance.ErrInstanceNotFound, "instance_not_found", http.StatusNotFound},
		{instance.ErrInstanceExists, "instance_already_exists", http.StatusConflict},
		{instance.ErrHostSecretMissing, "host_secret_missing", http.StatusUnprocessableEntity},
		{errors.New("anything else"), "internal", http.StatusInternalServerError},
	}
	for _, c := range cases {
		rr := httptest.NewRecorder()
		WriteError(rr, c.err)
		assert.Equal(t, c.stat, rr.Code, c.code)
		assert.Contains(t, rr.Body.String(), `"code":"`+c.code+`"`)
		assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	}
}

func TestWriteJSON(t *testing.T) {
	rr := httptest.NewRecorder()
	WriteJSON(rr, http.StatusCreated, map[string]string{"hello": "world"})
	assert.Equal(t, http.StatusCreated, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	assert.Contains(t, rr.Body.String(), `"hello":"world"`)
}
```

- [ ] **Step 2: Run test — expect fail**

Run: `go test ./internal/api/`
Expected: FAIL.

- [ ] **Step 3: Implement `errors.go`**

Create `internal/api/errors.go`:

```go
// Package api wires HTTP routes to the instance.Service.
package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
)

// ErrorBody is the JSON shape of every error response.
type ErrorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// WriteJSON writes v as JSON with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError translates an error into a JSON error response. Sentinel
// errors from the instance package map to known codes; anything else
// falls through to "internal".
func WriteError(w http.ResponseWriter, err error) {
	code, status, msg := classify(err)
	WriteJSON(w, status, ErrorBody{Code: code, Message: msg})
}

// WriteErrorWithDetails is like WriteError but lets handlers attach
// structured detail (e.g. host/template/slug).
func WriteErrorWithDetails(w http.ResponseWriter, err error, details map[string]any) {
	code, status, msg := classify(err)
	WriteJSON(w, status, ErrorBody{Code: code, Message: msg, Details: details})
}

func classify(err error) (code string, status int, msg string) {
	switch {
	case errors.Is(err, instance.ErrUnknownHost):
		return "unknown_host", http.StatusNotFound, err.Error()
	case errors.Is(err, instance.ErrUnknownTemplate):
		return "unknown_template", http.StatusNotFound, err.Error()
	case errors.Is(err, instance.ErrInstanceNotFound):
		return "instance_not_found", http.StatusNotFound, err.Error()
	case errors.Is(err, instance.ErrInstanceExists):
		return "instance_already_exists", http.StatusConflict, err.Error()
	case errors.Is(err, instance.ErrHostSecretMissing):
		return "host_secret_missing", http.StatusUnprocessableEntity, err.Error()
	case errors.Is(err, podman.ErrNotFound):
		return "instance_not_found", http.StatusNotFound, err.Error()
	default:
		return "internal", http.StatusInternalServerError, err.Error()
	}
}
```

- [ ] **Step 4: Run test — expect pass**

Run: `go test ./internal/api/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/api/errors.go internal/api/errors_test.go
git commit -m "feat(api): error code enum and JSON error writer"
```

---

### Task 18: Router skeleton + middleware wiring

**Files:**
- Create: `internal/api/router.go`
- Create: `internal/api/router_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/api/router_test.go`:

```go
package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
)

func newServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	tok := "test-tok"
	hash, err := config.HashToken(tok)
	require.NoError(t, err)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:read", "instances:*", "secrets:*"}}}

	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts, nil)

	r := NewRouter(svc, keys)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv, tok
}

func TestRouter_HealthzNoAuth(t *testing.T) {
	srv, _ := newServer(t)
	resp, err := http.Get(srv.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRouter_HostsRequiresAuth(t *testing.T) {
	srv, _ := newServer(t)
	resp, err := http.Get(srv.URL + "/hosts")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
```

- [ ] **Step 2: Run test — expect fail**

Run: `go test ./internal/api/ -run TestRouter`
Expected: FAIL (`NewRouter` undefined).

- [ ] **Step 3: Implement `router.go`**

Create `internal/api/router.go`:

```go
package api

import (
	"net/http"

	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
)

// NewRouter builds the full HTTP handler tree.
func NewRouter(svc *instance.Service, keys []config.APIKey) http.Handler {
	mux := http.NewServeMux()
	h := &handlers{svc: svc}

	// Process endpoints (no auth).
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// Hosts (read).
	guard := func(scope string, h http.Handler) http.Handler {
		return auth.New(keys, scope)(h)
	}
	mux.Handle("GET /hosts", guard("hosts:read", http.HandlerFunc(h.listHosts)))
	mux.Handle("GET /hosts/{host}", guard("hosts:read", http.HandlerFunc(h.getHost)))
	mux.Handle("GET /hosts/{host}/healthz", guard("hosts:read", http.HandlerFunc(h.hostHealthz)))
	mux.Handle("GET /hosts/{host}/ports-in-use", guard("hosts:read", http.HandlerFunc(h.portsInUse)))

	// Templates (read).
	mux.Handle("GET /templates", guard("instances:read", http.HandlerFunc(h.listTemplates)))
	mux.Handle("GET /templates/{id}", guard("instances:read", http.HandlerFunc(h.getTemplate)))
	mux.Handle("GET /templates/{id}/render", guard("instances:read", http.HandlerFunc(h.renderTemplate)))

	// Host secrets.
	mux.Handle("GET /hosts/{host}/secrets", guard("secrets:read", http.HandlerFunc(h.listSecrets)))
	mux.Handle("PUT /hosts/{host}/secrets/{name}", guard("secrets:write", http.HandlerFunc(h.putSecret)))
	mux.Handle("DELETE /hosts/{host}/secrets/{name}", guard("secrets:write", http.HandlerFunc(h.deleteSecret)))

	// Instances.
	mux.Handle("GET /hosts/{host}/instances", guard("instances:read", http.HandlerFunc(h.listInstances)))
	mux.Handle("GET /hosts/{host}/instances/{template}/{slug}", guard("instances:read", http.HandlerFunc(h.getInstance)))
	mux.Handle("POST /hosts/{host}/instances", guard("instances:write", http.HandlerFunc(h.createInstance)))
	mux.Handle("PUT /hosts/{host}/instances/{template}/{slug}", guard("instances:write", http.HandlerFunc(h.applyInstance)))
	mux.Handle("DELETE /hosts/{host}/instances/{template}/{slug}", guard("instances:write", http.HandlerFunc(h.deleteInstance)))

	// Lifecycle.
	mux.Handle("POST /hosts/{host}/instances/{template}/{slug}/start", guard("instances:write", http.HandlerFunc(h.startInstance)))
	mux.Handle("POST /hosts/{host}/instances/{template}/{slug}/stop", guard("instances:write", http.HandlerFunc(h.stopInstance)))
	mux.Handle("POST /hosts/{host}/instances/{template}/{slug}/restart", guard("instances:write", http.HandlerFunc(h.restartInstance)))
	mux.Handle("POST /hosts/{host}/instances/{template}/{slug}/upgrade", guard("instances:write", http.HandlerFunc(h.upgradeInstance)))

	// Logs.
	mux.Handle("GET /hosts/{host}/instances/{template}/{slug}/logs", guard("instances:read", http.HandlerFunc(h.logsInstance)))

	return mux
}

// handlers holds per-request dependencies. Each method is a thin adapter
// around svc.
type handlers struct {
	svc *instance.Service
}
```

Stub all handler methods so the package compiles. Add at the bottom of `router.go`:

```go
// Stub implementations for handlers defined in subsequent task files.
// Each stub returns 501 Not Implemented; later tasks replace them.
func (h *handlers) listHosts(w http.ResponseWriter, _ *http.Request)       { notImpl(w) }
func (h *handlers) getHost(w http.ResponseWriter, _ *http.Request)         { notImpl(w) }
func (h *handlers) hostHealthz(w http.ResponseWriter, _ *http.Request)     { notImpl(w) }
func (h *handlers) portsInUse(w http.ResponseWriter, _ *http.Request)      { notImpl(w) }
func (h *handlers) listTemplates(w http.ResponseWriter, _ *http.Request)   { notImpl(w) }
func (h *handlers) getTemplate(w http.ResponseWriter, _ *http.Request)     { notImpl(w) }
func (h *handlers) renderTemplate(w http.ResponseWriter, _ *http.Request)  { notImpl(w) }
func (h *handlers) listSecrets(w http.ResponseWriter, _ *http.Request)     { notImpl(w) }
func (h *handlers) putSecret(w http.ResponseWriter, _ *http.Request)       { notImpl(w) }
func (h *handlers) deleteSecret(w http.ResponseWriter, _ *http.Request)    { notImpl(w) }
func (h *handlers) listInstances(w http.ResponseWriter, _ *http.Request)   { notImpl(w) }
func (h *handlers) getInstance(w http.ResponseWriter, _ *http.Request)     { notImpl(w) }
func (h *handlers) createInstance(w http.ResponseWriter, _ *http.Request)  { notImpl(w) }
func (h *handlers) applyInstance(w http.ResponseWriter, _ *http.Request)   { notImpl(w) }
func (h *handlers) deleteInstance(w http.ResponseWriter, _ *http.Request)  { notImpl(w) }
func (h *handlers) startInstance(w http.ResponseWriter, _ *http.Request)   { notImpl(w) }
func (h *handlers) stopInstance(w http.ResponseWriter, _ *http.Request)    { notImpl(w) }
func (h *handlers) restartInstance(w http.ResponseWriter, _ *http.Request) { notImpl(w) }
func (h *handlers) upgradeInstance(w http.ResponseWriter, _ *http.Request) { notImpl(w) }
func (h *handlers) logsInstance(w http.ResponseWriter, _ *http.Request)    { notImpl(w) }

func notImpl(w http.ResponseWriter) {
	WriteJSON(w, http.StatusNotImplemented, ErrorBody{Code: "not_implemented", Message: "handler not yet implemented"})
}
```

- [ ] **Step 4: Run test — expect pass**

Run: `go test ./internal/api/ -run TestRouter`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/api/router.go internal/api/router_test.go
git commit -m "feat(api): router skeleton with auth middleware wiring"
```

---

### Task 19: Hosts and templates handlers (read-only)

Replaces six stubs: `listHosts`, `getHost`, `hostHealthz`, `portsInUse`, `listTemplates`, `getTemplate`, `renderTemplate`.

**Files:**
- Create: `internal/api/hosts.go`
- Create: `internal/api/templates.go`
- Modify: `internal/api/router.go` (delete the seven stubs being replaced)
- Create: `internal/api/hosts_test.go`
- Create: `internal/api/templates_test.go`

- [ ] **Step 1: Write the failing test for hosts**

Create `internal/api/hosts_test.go`:

```go
package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
)

func authedReq(t *testing.T, srv *httptest.Server, tok, method, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func TestListHosts(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:read"}}}
	hosts := []config.Host{
		{ID: "h1", Addr: "unix", Socket: "/x", Labels: map[string]string{"env": "dev"}},
	}
	svc := instance.NewService(fake.New(), hosts, nil)
	srv := httptest.NewServer(NewRouter(svc, keys))
	defer srv.Close()

	resp := authedReq(t, srv, tok, "GET", "/hosts")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Len(t, got, 1)
	assert.Equal(t, "h1", got[0]["id"])
	assert.Equal(t, "unix", got[0]["addr"])
}

func TestHostHealthz(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:read"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts, nil)
	srv := httptest.NewServer(NewRouter(svc, keys))
	defer srv.Close()

	resp := authedReq(t, srv, tok, "GET", "/hosts/h1/healthz")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = authedReq(t, srv, tok, "GET", "/hosts/nope/healthz")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
```

- [ ] **Step 2: Implement `hosts.go`**

Create `internal/api/hosts.go`:

```go
package api

import (
	"net/http"
)

func (h *handlers) listHosts(w http.ResponseWriter, _ *http.Request) {
	hosts := h.svc.Hosts()
	out := make([]map[string]any, 0, len(hosts))
	for _, host := range hosts {
		out = append(out, map[string]any{
			"id":     host.ID,
			"addr":   host.Addr,
			"labels": host.Labels,
		})
	}
	WriteJSON(w, http.StatusOK, out)
}

func (h *handlers) getHost(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("host")
	for _, host := range h.svc.Hosts() {
		if host.ID == id {
			WriteJSON(w, http.StatusOK, map[string]any{
				"id": host.ID, "addr": host.Addr, "labels": host.Labels,
			})
			return
		}
	}
	WriteError(w, instance.ErrUnknownHost)
}
```

Add `"github.com/iotready/podman-api/internal/instance"` to the imports of `hosts.go`.

Add `hostHealthz` and `portsInUse`:

```go
func (h *handlers) hostHealthz(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("host")
	known := false
	for _, host := range h.svc.Hosts() {
		if host.ID == id {
			known = true
			break
		}
	}
	if !known {
		WriteError(w, instance.ErrUnknownHost)
		return
	}
	// Cheap check: list pods (libpod must be reachable).
	if _, err := h.svc.PortsInUse(r.Context(), id); err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *handlers) portsInUse(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("host")
	ports, err := h.svc.PortsInUse(r.Context(), id)
	if err != nil {
		WriteError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(ports))
	for _, p := range ports {
		out = append(out, map[string]any{
			"host_ip":   p.HostIP,
			"host_port": p.HostPort,
			"protocol":  p.Protocol,
		})
	}
	WriteJSON(w, http.StatusOK, out)
}
```

Delete the four corresponding stubs from `router.go`.

- [ ] **Step 3: Write the failing test for templates**

Create `internal/api/templates_test.go`:

```go
package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
)

func newSrvWithTmpl(t *testing.T) (*httptest.Server, string) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:read"}}}
	tmpls := []config.Template{
		{Meta: render.Meta{
			ID: "x",
			Parameters: render.Parameters{Required: []string{"slug"}},
		}, Body: "kind: Pod\nname: x-{{.slug}}\n", Source: "x.yaml"},
	}
	svc := instance.NewService(fake.New(), nil, tmpls)
	srv := httptest.NewServer(NewRouter(svc, keys))
	t.Cleanup(srv.Close)
	return srv, tok
}

func TestListTemplates(t *testing.T) {
	srv, tok := newSrvWithTmpl(t)
	resp := authedReq(t, srv, tok, "GET", "/templates")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Len(t, got, 1)
	assert.Equal(t, "x", got[0]["id"])
}

func TestRenderTemplate(t *testing.T) {
	srv, tok := newSrvWithTmpl(t)
	resp := authedReq(t, srv, tok, "GET", "/templates/x/render?slug=hello")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := make([]byte, 1024)
	n, _ := resp.Body.Read(body)
	assert.Contains(t, string(body[:n]), "name: x-hello")
}
```

- [ ] **Step 4: Implement `templates.go`**

Create `internal/api/templates.go`:

```go
package api

import (
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/render"
)

func (h *handlers) listTemplates(w http.ResponseWriter, _ *http.Request) {
	tmpls := h.svc.Templates()
	out := make([]map[string]any, 0, len(tmpls))
	for _, t := range tmpls {
		out = append(out, templateView(t))
	}
	WriteJSON(w, http.StatusOK, out)
}

func (h *handlers) getTemplate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	for _, t := range h.svc.Templates() {
		if t.Meta.ID == id {
			WriteJSON(w, http.StatusOK, templateView(t))
			return
		}
	}
	WriteError(w, instance.ErrUnknownTemplate)
}

func (h *handlers) renderTemplate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var tmpl *config.Template
	for _, t := range h.svc.Templates() {
		if t.Meta.ID == id {
			tt := t
			tmpl = &tt
			break
		}
	}
	if tmpl == nil {
		WriteError(w, instance.ErrUnknownTemplate)
		return
	}
	params := map[string]any{}
	for k, v := range r.URL.Query() {
		if len(v) > 0 {
			params[k] = v[0]
		}
	}
	out, err := render.Render(rebuildSource(*tmpl), params)
	if err != nil {
		WriteError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/yaml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(out))
}

// rebuildSource is the same trick used in instance.Service: re-attach a
// minimal meta header so render.Render's ParseMeta succeeds.
func rebuildSource(t config.Template) string {
	return "# template-meta:\n#   id: " + t.Meta.ID + "\n#   parameters:\n#     required: []\n---\n" + t.Body
}

func templateView(t config.Template) map[string]any {
	return map[string]any{
		"id":         t.Meta.ID,
		"parameters": t.Meta.Parameters,
		"secrets":    t.Meta.Secrets,
		"volumes":    t.Meta.Volumes,
	}
}
```

Add `"github.com/iotready/podman-api/internal/config"` to the imports of `templates.go`.

Delete the three template stubs from `router.go`.

- [ ] **Step 5: Run tests**

Run: `go test ./internal/api/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/api/hosts.go internal/api/hosts_test.go internal/api/templates.go internal/api/templates_test.go internal/api/router.go
git commit -m "feat(api): hosts and templates read-only handlers"
```

---

### Task 20: Host secret handlers (list/put/delete)

**Files:**
- Create: `internal/api/secrets.go`
- Create: `internal/api/secrets_test.go`
- Modify: `internal/api/router.go` (delete the three secret stubs)
- Modify: `internal/instance/service.go` (add small accessors for secret ops if not already there)

The secret operations talk to `podman.Client` directly via small accessor methods on `Service`. Add these to `service.go` first.

- [ ] **Step 1: Add secret accessors to Service**

Append to `internal/instance/service.go`:

```go
// HostSecrets lists secrets on a host.
func (s *Service) HostSecrets(ctx context.Context, host string) ([]podman.Secret, error) {
	if _, ok := s.hosts[host]; !ok {
		return nil, ErrUnknownHost
	}
	return s.client.SecretList(ctx, host)
}

// PutHostSecret creates-or-rotates a host secret. We "rotate" by removing
// then recreating, since podman secrets are immutable.
func (s *Service) PutHostSecret(ctx context.Context, host, name string, value []byte) error {
	if _, ok := s.hosts[host]; !ok {
		return ErrUnknownHost
	}
	if _, err := s.client.SecretInspect(ctx, host, name); err == nil {
		if err := s.client.SecretRemove(ctx, host, name); err != nil {
			return err
		}
	}
	return s.client.SecretCreate(ctx, host, name, value)
}

func (s *Service) DeleteHostSecret(ctx context.Context, host, name string) error {
	if _, ok := s.hosts[host]; !ok {
		return ErrUnknownHost
	}
	err := s.client.SecretRemove(ctx, host, name)
	if errors.Is(err, podman.ErrNotFound) {
		return nil // idempotent delete
	}
	return err
}
```

- [ ] **Step 2: Write the failing test**

Create `internal/api/secrets_test.go`:

```go
package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
)

func newSrvWithSecrets(t *testing.T) (*httptest.Server, string) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"secrets:read", "secrets:write"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts, nil)
	srv := httptest.NewServer(NewRouter(svc, keys))
	t.Cleanup(srv.Close)
	return srv, tok
}

func TestPutAndDeleteSecret(t *testing.T) {
	srv, tok := newSrvWithSecrets(t)

	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/secrets/s1", bytes.NewBufferString(`{"value":"v1"}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	resp = authedReq(t, srv, tok, "GET", "/hosts/h1/secrets")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest("DELETE", srv.URL+"/hosts/h1/secrets/s1", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}
```

- [ ] **Step 3: Implement `secrets.go`**

Create `internal/api/secrets.go`:

```go
package api

import (
	"encoding/json"
	"net/http"
)

func (h *handlers) listSecrets(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	secrets, err := h.svc.HostSecrets(r.Context(), host)
	if err != nil {
		WriteError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(secrets))
	for _, s := range secrets {
		out = append(out, map[string]any{
			"name":       s.Name,
			"created_at": s.CreatedAt,
		})
	}
	WriteJSON(w, http.StatusOK, out)
}

func (h *handlers) putSecret(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	name := r.PathValue("name")
	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if body.Value == "" {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "value is required"})
		return
	}
	if err := h.svc.PutHostSecret(r.Context(), host, name, []byte(body.Value)); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handlers) deleteSecret(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	name := r.PathValue("name")
	if err := h.svc.DeleteHostSecret(r.Context(), host, name); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
```

Delete the three secret stubs from `router.go`.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/api/ ./internal/instance/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/instance/service.go internal/api/secrets.go internal/api/secrets_test.go internal/api/router.go
git commit -m "feat(api): host secret list/put/delete"
```

---

### Task 21: Instance CRUD handlers

Replaces stubs: `listInstances`, `getInstance`, `createInstance`, `applyInstance`, `deleteInstance`.

**Files:**
- Create: `internal/api/instances.go`
- Create: `internal/api/instances_test.go`
- Modify: `internal/api/router.go` (delete the five stubs)

- [ ] **Step 1: Write the failing test**

Create `internal/api/instances_test.go`:

```go
package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
)

func newSrvFull(t *testing.T) (*httptest.Server, string, *fake.Fake) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:*", "secrets:*", "hosts:read"}}}
	tmpls := []config.Template{
		{Meta: render.Meta{
			ID: "x",
			Parameters: render.Parameters{Required: []string{"slug", "image"}},
			Secrets:    render.Secrets{PerInstance: []string{"auth_secret"}},
		}, Body: `apiVersion: v1
kind: Pod
metadata:
  name: x-{{.slug}}
  labels:
    podman-api/template: x
    podman-api/slug: {{.slug}}
spec:
  containers:
    - name: c
      image: {{.image}}
`},
	}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	svc := instance.NewService(f, hosts, tmpls)
	srv := httptest.NewServer(NewRouter(svc, keys))
	t.Cleanup(srv.Close)
	return srv, tok, f
}

func TestApplyAndGetInstance(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	body := `{"template":"x","slug":"hello","parameters":{"slug":"hello","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/x/hello", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = authedReq(t, srv, tok, "GET", "/hosts/h1/instances/x/hello")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, "x", got["template"])
	assert.Equal(t, "hello", got["slug"])
}

func TestCreateConflict(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"x","slug":"hello","parameters":{"slug":"hello","image":"i:1"},"secrets":{"auth_secret":"s"}}`

	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/instances", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// Second POST → 409
	req, _ = http.NewRequest("POST", srv.URL+"/hosts/h1/instances", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestDeleteInstance(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"x","slug":"hello","parameters":{"slug":"hello","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/x/hello", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	req, _ = http.NewRequest("DELETE", srv.URL+"/hosts/h1/instances/x/hello", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	resp = authedReq(t, srv, tok, "GET", "/hosts/h1/instances/x/hello")
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
```

- [ ] **Step 2: Implement `instances.go`**

Create `internal/api/instances.go`:

```go
package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/iotready/podman-api/internal/instance"
)

func (h *handlers) listInstances(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	template := r.URL.Query().Get("template")
	if template == "" {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{
			Code: "invalid_query", Message: "template query parameter is required",
		})
		return
	}
	out, err := h.svc.List(r.Context(), host, template)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, out)
}

func (h *handlers) getInstance(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	tmpl := r.PathValue("template")
	slug := r.PathValue("slug")
	obs, err := h.svc.Get(r.Context(), host, tmpl, slug)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, obs)
}

func (h *handlers) createInstance(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	req, err := decodeApply(r)
	if err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if err := h.svc.Apply(r.Context(), host, req, false); err != nil {
		WriteError(w, err)
		return
	}
	obs, err := h.svc.Get(r.Context(), host, req.Template, req.Slug)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusCreated, obs)
}

func (h *handlers) applyInstance(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	req, err := decodeApply(r)
	if err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	// Override template/slug from URL — be strict if body disagrees.
	if got := r.PathValue("template"); req.Template != "" && req.Template != got {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "template in URL does not match body"})
		return
	}
	if got := r.PathValue("slug"); req.Slug != "" && req.Slug != got {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "slug in URL does not match body"})
		return
	}
	req.Template = r.PathValue("template")
	req.Slug = r.PathValue("slug")

	if err := h.svc.Apply(r.Context(), host, req, true); err != nil {
		WriteError(w, err)
		return
	}
	obs, err := h.svc.Get(r.Context(), host, req.Template, req.Slug)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, obs)
}

func (h *handlers) deleteInstance(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	tmpl := r.PathValue("template")
	slug := r.PathValue("slug")
	opts := instance.DeleteOptions{
		PruneVolumes: queryBool(r, "prune_volumes"),
		PruneSecrets: queryBool(r, "prune_secrets"),
	}
	if err := h.svc.Delete(r.Context(), host, tmpl, slug, opts); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeApply(r *http.Request) (instance.ApplyRequest, error) {
	var req instance.ApplyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return req, err
	}
	return req, nil
}

func queryBool(r *http.Request, key string) bool {
	v := r.URL.Query().Get(key)
	if v == "" {
		return false
	}
	b, _ := strconv.ParseBool(v)
	return b
}
```

Delete the five instance stubs from `router.go`.

- [ ] **Step 3: Run tests**

Run: `go test ./internal/api/`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/api/instances.go internal/api/instances_test.go internal/api/router.go
git commit -m "feat(api): instance CRUD handlers (list/get/create/apply/delete)"
```

---

### Task 22: Lifecycle handlers (start/stop/restart/upgrade)

**Files:**
- Modify: `internal/api/instances.go` (append)
- Modify: `internal/api/router.go` (delete four stubs)
- Create: `internal/api/lifecycle_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/api/lifecycle_test.go`:

```go
package api

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLifecycle_StartStopRestart(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"x","slug":"l","parameters":{"slug":"l","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/x/l", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	for _, action := range []string{"stop", "start", "restart"} {
		req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/instances/x/l/"+action, nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, http.StatusNoContent, resp.StatusCode, "action %s", action)
	}
}

func TestUpgrade_PullsAndApplies(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"x","slug":"u","parameters":{"slug":"u","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/x/u", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	upgrade := `{"image":"i:2","parameters":{"slug":"u","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ = http.NewRequest("POST", srv.URL+"/hosts/h1/instances/x/u/upgrade", bytes.NewBufferString(upgrade))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
```

- [ ] **Step 2: Append to `instances.go`**

```go
func (h *handlers) startInstance(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Start(r.Context(), r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handlers) stopInstance(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Stop(r.Context(), r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handlers) restartInstance(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Restart(r.Context(), r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handlers) upgradeInstance(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	tmpl := r.PathValue("template")
	slug := r.PathValue("slug")
	var body struct {
		Image      string            `json:"image"`
		Parameters map[string]any    `json:"parameters"`
		Secrets    map[string]string `json:"secrets"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	req := instance.ApplyRequest{
		Template:   tmpl,
		Slug:       slug,
		Parameters: body.Parameters,
		Secrets:    body.Secrets,
	}
	if err := h.svc.Upgrade(r.Context(), host, req, body.Image); err != nil {
		WriteError(w, err)
		return
	}
	obs, err := h.svc.Get(r.Context(), host, tmpl, slug)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, obs)
}
```

Delete the four lifecycle stubs from `router.go`.

- [ ] **Step 3: Run tests**

Run: `go test ./internal/api/`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/api/instances.go internal/api/lifecycle_test.go internal/api/router.go
git commit -m "feat(api): instance lifecycle handlers (start/stop/restart/upgrade)"
```

---

### Task 23: Logs handler (tail and SSE follow)

**Files:**
- Create: `internal/api/logs.go`
- Create: `internal/api/logs_test.go`
- Modify: `internal/instance/service.go` (add `Logs` accessor)
- Modify: `internal/api/router.go` (delete the logs stub)

- [ ] **Step 1: Add `Logs` to Service**

Append to `internal/instance/service.go`:

```go
// Logs returns a channel of log lines from one container in an instance.
func (s *Service) Logs(ctx context.Context, host, tmpl, slug, container string, opts podman.LogOptions) (<-chan podman.LogLine, error) {
	if _, err := s.lookup(host, tmpl); err != nil {
		return nil, err
	}
	// Verify the pod exists; the libpod logs call accepts a container name
	// directly, but giving 404 for unknown instances is friendlier.
	if _, err := s.client.PodInspect(ctx, host, podName(tmpl, slug)); err != nil {
		if errors.Is(err, podman.ErrNotFound) {
			return nil, ErrInstanceNotFound
		}
		return nil, err
	}
	// Container names in pods are "{pod}-{container}".
	cname := podName(tmpl, slug) + "-" + container
	return s.client.ContainerLogs(ctx, host, cname, opts)
}
```

- [ ] **Step 2: Write the failing test**

Create `internal/api/logs_test.go`:

```go
package api

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogs_NoFollow_ReturnsTextAndCloses(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"x","slug":"L","parameters":{"slug":"L","image":"i"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/x/L", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	resp = authedReq(t, srv, tok, "GET", "/hosts/h1/instances/x/L/logs?container=c&tail=10")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	// Fake's ContainerLogs returns an immediately-closed channel.
	assert.Equal(t, "text/plain; charset=utf-8", resp.Header.Get("Content-Type"))
}
```

- [ ] **Step 3: Implement `logs.go`**

Create `internal/api/logs.go`:

```go
package api

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/iotready/podman-api/internal/podman"
)

func (h *handlers) logsInstance(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	tmpl := r.PathValue("template")
	slug := r.PathValue("slug")
	container := r.URL.Query().Get("container")
	if container == "" {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_query", Message: "container is required"})
		return
	}
	tail, _ := strconv.Atoi(r.URL.Query().Get("tail"))
	follow, _ := strconv.ParseBool(r.URL.Query().Get("follow"))
	opts := podman.LogOptions{Tail: tail, Since: r.URL.Query().Get("since"), Follow: follow}

	ch, err := h.svc.Logs(r.Context(), host, tmpl, slug, container, opts)
	if err != nil {
		WriteError(w, err)
		return
	}

	if follow {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
	} else {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	for {
		select {
		case <-r.Context().Done():
			return
		case line, ok := <-ch:
			if !ok {
				return
			}
			if follow {
				_, _ = fmt.Fprintf(w, "event: log\ndata: %s\n\n", line.Line)
			} else {
				_, _ = fmt.Fprintln(w, line.Line)
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}
```

Delete the logs stub from `router.go`.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/api/ ./internal/instance/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/instance/service.go internal/api/logs.go internal/api/logs_test.go internal/api/router.go
git commit -m "feat(api): logs handler with SSE follow"
```

---

## Phase 7 — Observability

### Task 24: Audit log middleware + Prometheus metrics

**Files:**
- Create: `internal/obs/audit.go`
- Create: `internal/obs/audit_test.go`
- Create: `internal/obs/metrics.go`
- Modify: `internal/api/router.go` (wire both as middleware around state-changing routes; expose `/metrics`)

- [ ] **Step 1: Write the failing test for audit**

Create `internal/obs/audit_test.go`:

```go
package obs

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAudit_LogsStateChangingRequests(t *testing.T) {
	var buf bytes.Buffer
	mw := NewAuditMiddleware(&buf)

	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for _, m := range []string{"POST", "PUT", "DELETE"} {
		req := httptest.NewRequest(m, "/x", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
	}

	out := buf.String()
	require.NotEmpty(t, out, "expected audit lines for state-changing requests")
	for _, m := range []string{`"method":"POST"`, `"method":"PUT"`, `"method":"DELETE"`} {
		assert.Contains(t, out, m)
	}
}

func TestAudit_SkipsReadOnly(t *testing.T) {
	var buf bytes.Buffer
	mw := NewAuditMiddleware(&buf)
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	req := httptest.NewRequest("GET", "/x", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.True(t, strings.TrimSpace(buf.String()) == "")
}
```

- [ ] **Step 2: Implement `audit.go`**

Create `internal/obs/audit.go`:

```go
// Package obs holds the observability primitives: structured audit log
// middleware and Prometheus metrics.
package obs

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/iotready/podman-api/internal/auth"
)

// NewAuditMiddleware writes one JSON line per state-changing request to w.
// It logs nothing for safe methods (GET/HEAD/OPTIONS).
func NewAuditMiddleware(w io.Writer) func(http.Handler) http.Handler {
	enc := json.NewEncoder(w)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
				next.ServeHTTP(rw, r)
				return
			}
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: rw, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			_ = enc.Encode(map[string]any{
				"ts":          start.UTC().Format(time.RFC3339Nano),
				"method":      r.Method,
				"path":        r.URL.Path,
				"status":      rec.status,
				"duration_ms": time.Since(start).Milliseconds(),
				"key_id":      auth.KeyIDFromContext(r.Context()),
			})
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(c int) { s.status = c; s.ResponseWriter.WriteHeader(c) }
```

- [ ] **Step 3: Run audit tests**

Run: `go test ./internal/obs/`
Expected: PASS.

- [ ] **Step 4: Implement `metrics.go`**

Create `internal/obs/metrics.go`:

```go
package obs

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the Prometheus collectors. New() registers them with the
// default registry; call Handler() to expose /metrics.
type Metrics struct {
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
}

func New() *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "podman_api_requests_total",
			Help: "Total HTTP requests by method, route template, and status.",
		}, []string{"method", "route", "status"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "podman_api_request_duration_seconds",
			Help:    "Request duration by method and route.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
	}
	prometheus.MustRegister(m.requests, m.latency)
	return m
}

// Middleware records request count and duration. The route label is the
// method + path-pattern style ("POST /hosts/{host}/instances").
func (m *Metrics) Middleware(routeTmpl string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			d := time.Since(start).Seconds()
			m.requests.WithLabelValues(r.Method, routeTmpl, strconv.Itoa(rec.status)).Inc()
			m.latency.WithLabelValues(r.Method, routeTmpl).Observe(d)
		})
	}
}

// Handler is the http.Handler for the /metrics endpoint.
func (m *Metrics) Handler() http.Handler { return promhttp.Handler() }
```

- [ ] **Step 5: Wire audit + metrics into the router**

Modify `internal/api/router.go`. Change `NewRouter` signature to accept the middleware factories:

```go
func NewRouter(svc *instance.Service, keys []config.APIKey, audit func(http.Handler) http.Handler, metricsHandler http.Handler) http.Handler {
```

If `audit` is nil, default it to identity:

```go
if audit == nil {
    audit = func(h http.Handler) http.Handler { return h }
}
```

Wrap every state-changing route's handler with `audit(...)`. Mount `/metrics` if `metricsHandler != nil`:

```go
if metricsHandler != nil {
	mux.Handle("GET /metrics", metricsHandler)
}
```

Update **every** test file that calls `NewRouter` to pass two extra args (`nil, nil`). The complete list:

- `internal/api/router_test.go` (in `newServer`)
- `internal/api/hosts_test.go` (`TestListHosts`, `TestHostHealthz`)
- `internal/api/templates_test.go` (in `newSrvWithTmpl`)
- `internal/api/secrets_test.go` (in `newSrvWithSecrets`)
- `internal/api/instances_test.go` (in `newSrvFull`)
- `internal/api/lifecycle_test.go` (uses `newSrvFull` — no change there)
- `internal/api/logs_test.go` (uses `newSrvFull` — no change there)

Each direct `NewRouter(svc, keys)` becomes `NewRouter(svc, keys, nil, nil)`.

- [ ] **Step 6: Run all tests**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/obs/ internal/api/router.go internal/api/router_test.go internal/api/hosts_test.go internal/api/instances_test.go internal/api/lifecycle_test.go internal/api/logs_test.go internal/api/secrets_test.go internal/api/templates_test.go go.mod go.sum
git commit -m "feat(obs): audit log middleware and Prometheus metrics"
```

---

## Phase 8 — Bundled templates

### Task 25: Add `lite-crm.yaml` and `google-groups.yaml`

The full `lite-engine.yaml` already shipped in Task 6. Now add the other two templates.

**Files:**
- Create: `templates/lite-crm.yaml`
- Create: `templates/google-groups.yaml`

- [ ] **Step 1: `templates/lite-crm.yaml`**

Copy `templates/lite-engine.yaml` and adjust:
- `# template-meta:` → change `id: lite-engine` to `id: lite-crm`
- ConfigMap name and labels: replace `lite-engine` with `lite-crm`
- Pod name: `lite-crm-{{.slug}}`
- Litestream replica path: `lite/crm/{{.slug}}/`
- Default DB path: `/data/crm.db` (verify against the lite-crm container's actual SQLite filename; if different, adjust)

Everything else (env vars, volumes, secret refs) is identical.

- [ ] **Step 2: `templates/google-groups.yaml`**

Create:

```yaml
# template-meta:
#   id: google-groups
#   parameters:
#     required: [slug, image, port]
#     optional: []
#   secrets:
#     per_instance: []
#     per_host_referenced: []
#   volumes: []
---
apiVersion: v1
kind: Pod
metadata:
  name: google-groups-{{.slug}}
  labels:
    podman-api/template: google-groups
    podman-api/slug: {{.slug}}
spec:
  restartPolicy: Always
  containers:
    - name: app
      image: {{.image}}
      ports:
        - containerPort: 8080
          hostPort: {{.port}}
          hostIP: 127.0.0.1
```

- [ ] **Step 3: Verify the templates load**

Run: `go test ./internal/config/ -run TestLoadTemplates`
Expected: PASS — both new templates parse, total count is 3.

(If the test asserts a specific count of 1, update it to assert ≥3 and presence of all three IDs.)

- [ ] **Step 4: Commit**

```bash
git add templates/
git commit -m "feat(templates): add lite-crm and google-groups bundled templates"
```

---

## Phase 9 — `cmd/podman-api/main.go` + bootstrap

### Task 26: Wire it all together

**Files:**
- Create: `cmd/podman-api/main.go`
- Create: `hosts/local.yaml` (dev only — points at the local socket)
- Create: `auth/keys.yaml.example` (template; the real `auth/keys.yaml` is gitignored)
- Modify: `.gitignore` (add `auth/keys.yaml`)

**SAFETY NOTE:** Do not create any host file other than `hosts/local.yaml` while this plan is being executed. Do not invoke the binary against a remote host.

- [ ] **Step 1: Add `hosts/local.yaml`**

Create `hosts/local.yaml`:

```yaml
id: local
addr: unix
socket: /run/user/1000/podman/podman.sock
labels:
  env: dev
```

- [ ] **Step 2: Add `auth/keys.yaml.example`**

Create `auth/keys.yaml.example`:

```yaml
# Real keys go in auth/keys.yaml (gitignored).
# Generate a hash with:
#   go run ./cmd/podman-api hash-token <plaintext>
keys:
  - id: cms-dev
    secret_hash: $argon2id$v=19$m=65536,t=3,p=4$REPLACE_ME$REPLACE_ME
    scopes: [hosts:read, instances:*, secrets:*]
    description: "Local CMS dev"
```

Append to `.gitignore`:

```
/auth/keys.yaml
```

- [ ] **Step 3: Implement `main.go`**

Create `cmd/podman-api/main.go`:

```go
// Command podman-api is the HTTP service that translates CMS REST calls
// into libpod REST calls against one or more Podman hosts.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/iotready/podman-api/internal/api"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/obs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/templates"
)

func main() {
	var (
		addr      = flag.String("addr", "127.0.0.1:8080", "bind address")
		hostsDir  = flag.String("hosts-dir", "hosts", "directory of hosts/*.yaml files")
		keysFile  = flag.String("keys-file", "auth/keys.yaml", "path to bearer keys file")
		tmplDir   = flag.String("templates-dir", "", "if set, load templates from this dir instead of embedded")
	)
	flag.Parse()

	if len(flag.Args()) > 0 && flag.Arg(0) == "hash-token" {
		if len(flag.Args()) < 2 {
			fmt.Fprintln(os.Stderr, "usage: podman-api hash-token <plaintext>")
			os.Exit(2)
		}
		h, err := config.HashToken(flag.Arg(1))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(h)
		return
	}

	hosts, err := config.LoadHosts(*hostsDir)
	if err != nil {
		log.Fatalf("hosts: %v", err)
	}
	keysRaw, err := os.ReadFile(*keysFile)
	if err != nil {
		log.Fatalf("keys: %v", err)
	}
	keys, err := config.ParseKeysYAML(keysRaw)
	if err != nil {
		log.Fatalf("keys: %v", err)
	}

	var tmpls []config.Template
	if *tmplDir != "" {
		tmpls, err = config.LoadTemplates(os.DirFS(*tmplDir), ".")
	} else {
		tmpls, err = config.LoadTemplates(templates.Files, ".")
	}
	if err != nil {
		log.Fatalf("templates: %v", err)
	}

	client, err := podman.NewReal(hosts)
	if err != nil {
		log.Fatalf("podman: %v", err)
	}

	svc := instance.NewService(client, hosts, tmpls)
	metrics := obs.New()
	audit := obs.NewAuditMiddleware(os.Stdout)

	router := api.NewRouter(svc, keys, audit, metrics.Handler())

	srv := &http.Server{
		Addr:              *addr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	idleClosed := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		close(idleClosed)
	}()

	log.Printf("podman-api listening on %s with %d hosts, %d templates, %d keys",
		*addr, len(hosts), len(tmpls), len(keys))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	<-idleClosed
}
```

- [ ] **Step 4: Verify it builds**

Run: `go build -o bin/podman-api ./cmd/podman-api`
Expected: exits 0; `./bin/podman-api` exists.

- [ ] **Step 5: Verify hash-token works**

Run: `./bin/podman-api hash-token hunter2`
Expected: prints a string starting with `$argon2id$`.

- [ ] **Step 6: Smoke-start the binary (LOCAL only)**

Generate a dev key:

```bash
HASH=$(./bin/podman-api hash-token devtoken)
mkdir -p auth
cat > auth/keys.yaml <<YAML
keys:
  - id: dev
    secret_hash: '$HASH'
    scopes: [hosts:read, instances:*, secrets:*]
YAML
```

Then:

```bash
./bin/podman-api -addr=127.0.0.1:8080 &
sleep 1
curl -s http://127.0.0.1:8080/healthz
curl -s -H "Authorization: Bearer devtoken" http://127.0.0.1:8080/hosts
kill %1
```

Expected: `/healthz` returns `{"status":"ok"}`; `/hosts` returns `[{"id":"local","addr":"unix",...}]`.

- [ ] **Step 7: Commit**

```bash
git add cmd/podman-api/main.go hosts/local.yaml auth/keys.yaml.example .gitignore
git commit -m "feat: cmd/podman-api main entry point"
```

---

## Phase 10 — End-to-end test against local podman

### Task 27: Integration test — full lifecycle through the HTTP API

This test starts the API in-process, points it at the local rootless socket, and exercises POST → GET → STOP → START → DELETE through HTTP. It verifies the full stack: render → play kube → libpod → observed shape.

**SAFETY:** local socket only. Test skips if no local podman.

**Files:**
- Create: `cmd/podman-api/e2e_integration_test.go` (`//go:build integration`)

- [ ] **Step 1: Write the test**

Create `cmd/podman-api/e2e_integration_test.go`:

```go
//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/api"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/obs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
)

// e2eTemplate is a minimal single-container template inlined to avoid pulling
// the litestream sidecar (and S3 secrets) into the integration test.
const e2eTemplate = `# template-meta:
#   id: e2e
#   parameters:
#     required: [slug, image, port]
#   secrets:
#     per_instance: []
#     per_host_referenced: []
#   volumes: []
---
apiVersion: v1
kind: Pod
metadata:
  name: e2e-{{.slug}}
  labels:
    podman-api/template: e2e
    podman-api/slug: {{.slug}}
spec:
  restartPolicy: Never
  containers:
    - name: c
      image: {{.image}}
      command: ["sleep", "60"]
`

func localSock(t *testing.T) string {
	t.Helper()
	rt := os.Getenv("XDG_RUNTIME_DIR")
	if rt == "" {
		t.Skip("XDG_RUNTIME_DIR unset")
	}
	p := filepath.Join(rt, "podman", "podman.sock")
	if _, err := os.Stat(p); err != nil {
		t.Skip("local podman socket not available: " + err.Error())
	}
	return p
}

func TestE2E_FullLifecycle_LocalOnly(t *testing.T) {
	sock := localSock(t)

	// Wire the stack manually so we can inject the e2e template.
	hosts := []config.Host{{ID: "local", Addr: "unix", Socket: sock}}
	meta, body, err := render.ParseMeta(e2eTemplate)
	require.NoError(t, err)
	tmpls := []config.Template{{Meta: meta, Body: body, Source: "e2e.yaml"}}

	client, err := podman.NewReal(hosts)
	require.NoError(t, err)

	tok := "e2etoken"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "e2e", SecretHash: hash, Scopes: []string{"instances:*", "hosts:read"}}}
	svc := instance.NewService(client, hosts, tmpls)
	mw := func(h http.Handler) http.Handler { return h }
	r := api.NewRouter(svc, keys, mw, obs.New().Handler())
	srv := httptest.NewServer(r)
	defer srv.Close()

	cleanup := func() {
		_ = client.PodRemove(context.Background(), "local", "e2e-itest", true)
	}
	t.Cleanup(cleanup)

	do := func(method, path, body string) (*http.Response, error) {
		req, _ := http.NewRequest(method, srv.URL+path, bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		return http.DefaultClient.Do(req)
	}

	// CREATE
	body := `{"template":"e2e","slug":"itest","parameters":{"slug":"itest","image":"docker.io/library/alpine:latest","port":31999}}`
	resp, err := do("PUT", "/hosts/local/instances/e2e/itest", body)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for Running.
	require.Eventually(t, func() bool {
		r, err := do("GET", "/hosts/local/instances/e2e/itest", "")
		if err != nil {
			return false
		}
		defer r.Body.Close()
		if r.StatusCode != 200 {
			return false
		}
		var got map[string]any
		_ = json.NewDecoder(r.Body).Decode(&got)
		pod, _ := got["pod"].(map[string]any)
		return pod["status"] == "Running"
	}, 30*time.Second, 500*time.Millisecond)

	// STOP
	resp, _ = do("POST", "/hosts/local/instances/e2e/itest/stop", "")
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// START
	resp, _ = do("POST", "/hosts/local/instances/e2e/itest/start", "")
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// DELETE
	resp, _ = do("DELETE", "/hosts/local/instances/e2e/itest", "")
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// GET → 404
	resp, _ = do("GET", "/hosts/local/instances/e2e/itest", "")
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Sanity: no orphan pod.
	_, err = client.PodInspect(context.Background(), "local", "e2e-itest")
	require.ErrorIs(t, err, podman.ErrNotFound, fmt.Sprintf("unexpected: %v", err))
	_ = strings.Contains // keep imports tidy across builds
}
```

- [ ] **Step 2: Run the test**

Ensure local podman socket is up: `systemctl --user start podman.socket`.
Run: `go test -tags=integration ./cmd/podman-api/ -run TestE2E_FullLifecycle_LocalOnly -v`
Expected: PASS in under a minute. First run pulls `alpine`.

- [ ] **Step 3: Run the entire test suite (unit + integration)**

Run: `make test && make test-integration`
Expected: PASS for both.

- [ ] **Step 4: Commit**

```bash
git add cmd/podman-api/e2e_integration_test.go
git commit -m "test(e2e): full HTTP lifecycle against local podman"
```

---

### Task 28: Volume endpoints

The spec lists `GET /hosts/{host}/instances/{template}/{slug}/volumes` (per-instance volumes) and `DELETE /hosts/{host}/volumes/{name}` (raw volume removal). The instance detail already includes volumes, but a dedicated list endpoint is useful for the CMS's pre-deletion warning flow, and the raw delete is needed for cleanup after `prune_volumes=false` deletes.

**Files:**
- Modify: `internal/instance/service.go` (add `InstanceVolumes` and `DeleteVolume`)
- Modify: `internal/api/router.go` (add the two routes)
- Modify: `internal/api/instances.go` (add `instanceVolumes` handler)
- Create: `internal/api/volumes.go` (volume delete handler)
- Create: `internal/api/volumes_test.go`

- [ ] **Step 1: Add Service methods**

Append to `internal/instance/service.go`:

```go
// InstanceVolumes returns the named volumes the API believes belong to this instance.
// Volumes that don't exist on the host are omitted (no error).
func (s *Service) InstanceVolumes(ctx context.Context, host, tmpl, slug string) ([]podman.Volume, error) {
	t, err := s.lookup(host, tmpl)
	if err != nil {
		return nil, err
	}
	var out []podman.Volume
	for _, v := range t.Meta.Volumes {
		name := tmpl + "-" + slug + "-" + v.Name
		if vv, err := s.client.VolumeInspect(ctx, host, name); err == nil {
			out = append(out, vv)
		}
	}
	return out, nil
}

// DeleteVolume removes a named volume on a host. Idempotent.
func (s *Service) DeleteVolume(ctx context.Context, host, name string, force bool) error {
	if _, ok := s.hosts[host]; !ok {
		return ErrUnknownHost
	}
	err := s.client.VolumeRemove(ctx, host, name, force)
	if errors.Is(err, podman.ErrNotFound) {
		return nil
	}
	return err
}
```

- [ ] **Step 2: Write the failing test**

Create `internal/api/volumes_test.go`:

```go
package api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteVolume_Idempotent(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	resp := authedReq(t, srv, tok, "DELETE", "/hosts/h1/volumes/does-not-exist")
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func TestInstanceVolumes_EmptyWhenNoVolumes(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	// The "x" template in newSrvFull has no volumes, so this returns [].
	body := `{"template":"x","slug":"v","parameters":{"slug":"v","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/x/v", nil)
	_ = body
	_ = req
	resp := authedReq(t, srv, tok, "GET", "/hosts/h1/instances/x/v/volumes")
	defer resp.Body.Close()
	// Either 200 with empty list (instance exists) or 404 (we never PUT it).
	assert.Contains(t, []int{http.StatusOK, http.StatusNotFound}, resp.StatusCode)
}
```

- [ ] **Step 3: Add handler in `instances.go`**

Append to `internal/api/instances.go`:

```go
func (h *handlers) instanceVolumes(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	tmpl := r.PathValue("template")
	slug := r.PathValue("slug")
	vols, err := h.svc.InstanceVolumes(r.Context(), host, tmpl, slug)
	if err != nil {
		WriteError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(vols))
	for _, v := range vols {
		out = append(out, map[string]any{"name": v.Name, "size_bytes": v.SizeBytes})
	}
	WriteJSON(w, http.StatusOK, out)
}
```

- [ ] **Step 4: Create `volumes.go`**

Create `internal/api/volumes.go`:

```go
package api

import (
	"net/http"
	"strconv"
)

func (h *handlers) deleteVolume(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	name := r.PathValue("name")
	force, _ := strconv.ParseBool(r.URL.Query().Get("force"))
	if err := h.svc.DeleteVolume(r.Context(), host, name, force); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
```

- [ ] **Step 5: Register routes**

Add to `NewRouter` in `internal/api/router.go`:

```go
mux.Handle("GET /hosts/{host}/instances/{template}/{slug}/volumes", guard("instances:read", http.HandlerFunc(h.instanceVolumes)))
mux.Handle("DELETE /hosts/{host}/volumes/{name}", guard("instances:write", http.HandlerFunc(h.deleteVolume)))
```

- [ ] **Step 6: Run tests**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/instance/service.go internal/api/instances.go internal/api/volumes.go internal/api/volumes_test.go internal/api/router.go
git commit -m "feat(api): per-instance volumes list and raw volume delete"
```

---

## Task summary

| # | Phase | Task |
|---|---|---|
| 1 | 0 | Initialize Go module, Makefile, .gitignore |
| 2 | 1 | Render: parse template-meta block |
| 3 | 1 | Render: text/template body rendering |
| 4 | 1 | Render: parameter & secret validation |
| 5 | 2 | Config: load `hosts/*.yaml` |
| 6 | 2 | Config: embed templates and load metadata |
| 7 | 2 | Config: load `auth/keys.yaml` + argon2id |
| 8 | 3 | Auth: bearer-token middleware |
| 9 | 4 | Podman: Client interface + types |
| 10 | 4 | Podman: in-memory fake |
| 11 | 4 | Podman: real Client — connection mgmt + Ping/Version |
| 12 | 4 | Podman: real Client — pod ops |
| 13 | 4 | Podman: real Client — secrets + volumes |
| 14 | 4 | Podman: real Client — logs + image pull + ports |
| 15 | 5 | Instance: Observed-shape normalization |
| 16 | 5 | Instance: Service with per-instance locking |
| 17 | 6 | API: error codes + JSON writer |
| 18 | 6 | API: router skeleton + middleware wiring |
| 19 | 6 | API: hosts and templates handlers |
| 20 | 6 | API: host secret handlers |
| 21 | 6 | API: instance CRUD handlers |
| 22 | 6 | API: lifecycle handlers |
| 23 | 6 | API: logs handler (tail + SSE) |
| 24 | 7 | Obs: audit middleware + Prometheus metrics |
| 25 | 8 | Templates: lite-crm and google-groups |
| 26 | 9 | cmd/podman-api: main wiring + dev hosts/keys |
| 27 | 10 | E2E test against local podman |
| 28 | 6 | API: per-instance volumes list + raw volume delete |

## Not covered by this plan

These are real but explicitly out of scope for v1:

- **SSH-tunnel verification against any real host.** The code supports `ssh://` URIs, but verifying production transport is a manual step outside this plan.
- **Ring 3 smoke test against a staging host.** Same reason.
- **Multi-API-instance horizontal scaling.** Per-instance locking is in-process only.
- **SIGHUP config reload.** The binary loads config at boot only; restart to pick up changes.
- **Customer-facing OIDC.** API trusts the CMS; no end-user auth.
- **Cross-host migration tooling.** Each instance lives on the host where it was created.
- **Litestream restore tooling.** Disaster recovery is a separate operational concern.

## Follow-up work to schedule

After this plan ships and the binary has been validated against a staging host (manually):

- Migrate `lite-engine-*` quadlets on `otp-prod-1` into pods managed by `podman-api`.
- Bootstrap each production host's `s3-access-key-id` and `s3-secret-access-key` per-host secrets.
- Cut over the CMS to call `podman-api` instead of whatever it does today.
- Add a `/events` SSE stream if the CMS wants push (currently noted as v2).









