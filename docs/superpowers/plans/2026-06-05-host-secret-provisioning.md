# Per-host secret provisioning on the destination — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Auto-provision a template's `per_host_referenced` secrets on a migrate/evacuate destination from values persisted in the encrypted state store, instead of failing with `ErrHostSecretMissing`.

**Architecture:** `PUT /hosts/{host}/secrets/{name}` persists the value (sealed, keyed by `(host, name)`) by default when the store is enabled. On migrate A→B, per-host secrets absent on B are created from host A's stored value before `Apply`. A single store-decision helper (`hostSecretProvisionable`) keeps the preflight gate, the executor, and the evacuate plan-preview in lockstep.

**Tech Stack:** Go; `modernc.org/sqlite`; existing `seal`/`open` AES sealing + `KeyStore`; `testify`.

**Build/test:** Always use the Makefile (CGO driver tags). Single package while iterating:
`go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/ -run X -v`
Full suite: `make test`. Keep `gofmt -l .` empty and `go vet` clean.

**Spec:** `docs/superpowers/specs/2026-06-05-host-secret-provisioning-design.md`

---

## Background the implementer needs

- **Podman secrets are write-only** — `podman.Secret` carries only `Name`/`CreatedAt`; there is no read-value API. The destination value must come from the state store, never from a host.
- **Per-instance secrets already migrate** (their values live in `spec.Secrets`); this work is only about **per-host** secrets (`tmpl.Meta.Secrets.PerHostReferenced`), which the API previously only existence-checked.
- **Rollback is already safe**: `Delete(..., DeleteOptions{PruneSecrets: true})` prunes only `PerInstance` secrets (`internal/instance/service.go:485-489`). Provisioned host secrets are therefore never removed on rollback — no extra code needed.
- **The store is mandatory on the migrate path** (`Migrate` returns `ErrStoreDisabled` if `s.store == nil`, `internal/instance/migrate.go:205-207`), so executor code may call `s.store.*` directly. The PUT/DELETE routes and the plan-preview must still guard `s.store != nil`.
- `store.Store` has exactly two implementers: `*store.SQLite` (`internal/store/sqlite.go`) and `*store.Memory` (`internal/store/memory.go`). Both must implement any new interface method or the build breaks.

---

## File structure

| File | Responsibility | Change |
|------|----------------|--------|
| `internal/store/store.go` | `Store` interface | Add 3 method signatures |
| `internal/store/sqlite.go` | SQLite impl | `host_secrets` table in `schemaSQL`; 3 methods |
| `internal/store/memory.go` | in-memory double | `hostSecrets` map; 3 methods |
| `internal/store/host_secrets_test.go` | store tests | **Create** |
| `internal/instance/service.go` | `PutHostSecret`/`DeleteHostSecret` persistence | Modify signatures + bodies |
| `internal/instance/migrate.go` | `hostSecretProvisionable` helper; `preflightIssues` provisionable; `migratePostStop` provisioning | Modify |
| `internal/instance/host_secret_provision_test.go` | service + migrate tests | **Create** |
| `internal/instance/evacuate_plan.go` | `PlannedMove.Provisions` | Modify |
| `internal/api/secrets.go` | `putSecret` decode `persist` | Modify |
| `internal/api/router.go` | (no change) | — |
| `api/openapi.yaml` | `persist` body field; `provisions` schema | Modify |

---

## Task 1: Store layer — persist per-host secret values

**Files:**
- Modify: `internal/store/store.go` (interface, after `ListSpecKeys`)
- Modify: `internal/store/sqlite.go` (`schemaSQL` const; new methods)
- Modify: `internal/store/memory.go` (`Memory` struct, `NewMemory`, new methods)
- Test: `internal/store/host_secrets_test.go` (create)

- [ ] **Step 1: Write the failing test**

Create `internal/store/host_secrets_test.go`. This is in-package (`package store`) so it can drive both implementers through a shared table. Mirror the existing `sqlite_test.go` setup for opening a temp SQLite with a key.

```go
package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hostSecretStore is the slice of Store exercised here.
type hostSecretStore interface {
	PutHostSecret(ctx context.Context, host, name string, value []byte) error
	GetHostSecret(ctx context.Context, host, name string) ([]byte, error)
	DeleteHostSecret(ctx context.Context, host, name string) error
}

func hostSecretStores(t *testing.T) map[string]hostSecretStore {
	t.Helper()
	keys := &KeyStore{}
	keys.Store([32]byte{1, 2, 3})
	sq, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), keys)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sq.Close() })
	return map[string]hostSecretStore{"sqlite": sq, "memory": NewMemory()}
}

func TestHostSecret_RoundTrip(t *testing.T) {
	ctx := context.Background()
	for name, st := range hostSecretStores(t) {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, st.PutHostSecret(ctx, "h1", "shared-token", []byte("v1")))
			got, err := st.GetHostSecret(ctx, "h1", "shared-token")
			require.NoError(t, err)
			assert.Equal(t, []byte("v1"), got)
		})
	}
}

func TestHostSecret_Upsert(t *testing.T) {
	ctx := context.Background()
	for name, st := range hostSecretStores(t) {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, st.PutHostSecret(ctx, "h1", "k", []byte("old")))
			require.NoError(t, st.PutHostSecret(ctx, "h1", "k", []byte("new")))
			got, err := st.GetHostSecret(ctx, "h1", "k")
			require.NoError(t, err)
			assert.Equal(t, []byte("new"), got)
		})
	}
}

func TestHostSecret_NotFound(t *testing.T) {
	ctx := context.Background()
	for name, st := range hostSecretStores(t) {
		t.Run(name, func(t *testing.T) {
			_, err := st.GetHostSecret(ctx, "h1", "missing")
			assert.ErrorIs(t, err, ErrNotFound)
		})
	}
}

func TestHostSecret_ScopedByHost(t *testing.T) {
	ctx := context.Background()
	for name, st := range hostSecretStores(t) {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, st.PutHostSecret(ctx, "h1", "k", []byte("a")))
			require.NoError(t, st.PutHostSecret(ctx, "h2", "k", []byte("b")))
			g1, _ := st.GetHostSecret(ctx, "h1", "k")
			g2, _ := st.GetHostSecret(ctx, "h2", "k")
			assert.Equal(t, []byte("a"), g1)
			assert.Equal(t, []byte("b"), g2)
		})
	}
}

func TestHostSecret_Delete(t *testing.T) {
	ctx := context.Background()
	for name, st := range hostSecretStores(t) {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, st.PutHostSecret(ctx, "h1", "k", []byte("v")))
			require.NoError(t, st.DeleteHostSecret(ctx, "h1", "k"))
			_, err := st.GetHostSecret(ctx, "h1", "k")
			assert.ErrorIs(t, err, ErrNotFound)
			// Delete is idempotent: removing an absent key is not an error.
			require.NoError(t, st.DeleteHostSecret(ctx, "h1", "k"))
		})
	}
}

// SQLite seals at rest: the encrypted blob must not contain the plaintext.
func TestHostSecret_SQLiteSealsAtRest(t *testing.T) {
	keys := &KeyStore{}
	keys.Store([32]byte{9})
	sq, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), keys)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sq.Close() })
	ctx := context.Background()
	require.NoError(t, sq.PutHostSecret(ctx, "h1", "k", []byte("SUPERSECRET")))
	var blob []byte
	require.NoError(t, sq.db.QueryRowContext(ctx,
		`SELECT value FROM host_secrets WHERE host='h1' AND name='k'`).Scan(&blob))
	assert.NotContains(t, string(blob), "SUPERSECRET")
	assert.False(t, errors.Is(nil, ErrNotFound)) // keep errors import used
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/ -run TestHostSecret -v`
Expected: FAIL — compile error, `PutHostSecret`/`GetHostSecret`/`DeleteHostSecret` undefined.

- [ ] **Step 3: Add the interface methods**

In `internal/store/store.go`, add to the `Store` interface (after `ListSpecKeys`, before the closing `}`):

```go
	// PutHostSecret inserts or replaces the sealed value of a per-host secret,
	// keyed by (host, name). Implementations seal Value at rest.
	PutHostSecret(ctx context.Context, host, name string, value []byte) error
	// GetHostSecret returns the decrypted per-host secret value, or ErrNotFound.
	GetHostSecret(ctx context.Context, host, name string) ([]byte, error)
	// DeleteHostSecret removes a per-host secret; absent is not an error.
	DeleteHostSecret(ctx context.Context, host, name string) error
```

- [ ] **Step 4: Add the SQLite table and methods**

In `internal/store/sqlite.go`, extend `schemaSQL` — add this block before the closing backtick (after the `jobs_state` index):

```sql
CREATE TABLE IF NOT EXISTS host_secrets (
  host    TEXT NOT NULL,
  name    TEXT NOT NULL,
  value   BLOB NOT NULL,
  created INTEGER NOT NULL,
  updated INTEGER NOT NULL,
  PRIMARY KEY (host, name)
);
```

Then add the methods (place after `DeleteSpec`/`ListSpecKeys`, modeled on `PutSpec`/`GetSpec`):

```go
func (s *SQLite) PutHostSecret(ctx context.Context, host, name string, value []byte) error {
	blob, err := seal(s.keys.Load(), value)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	return s.write(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `
INSERT INTO host_secrets (host, name, value, created, updated)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(host, name) DO UPDATE SET
  value   = excluded.value,
  updated = excluded.updated`,
			host, name, blob, now, now)
		return err
	})
}

func (s *SQLite) GetHostSecret(ctx context.Context, host, name string) ([]byte, error) {
	var blob []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT value FROM host_secrets WHERE host=? AND name=?`, host, name).Scan(&blob)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return open(s.keys.Load(), blob)
}

func (s *SQLite) DeleteHostSecret(ctx context.Context, host, name string) error {
	return s.write(ctx, func() error {
		_, err := s.db.ExecContext(ctx,
			`DELETE FROM host_secrets WHERE host=? AND name=?`, host, name)
		return err
	})
}
```

(`seal`/`open`, `s.keys`, `s.write`, `sql`, `time`, `errors` are all already in this file/package.)

- [ ] **Step 5: Add the memory methods**

In `internal/store/memory.go`, add a field to the `Memory` struct (alongside `specs`):

```go
	hostSecrets map[string][]byte // "host\x00name" -> value
```

Initialize it in `NewMemory` (alongside `specs: map[string]Spec{}`):

```go
		hostSecrets: map[string][]byte{},
```

Add the methods (after `ListSpecKeys`):

```go
func (m *Memory) PutHostSecret(_ context.Context, host, name string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := append([]byte(nil), value...)
	m.hostSecrets[host+"\x00"+name] = cp
	return nil
}

func (m *Memory) GetHostSecret(_ context.Context, host, name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.hostSecrets[host+"\x00"+name]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

func (m *Memory) DeleteHostSecret(_ context.Context, host, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.hostSecrets, host+"\x00"+name)
	return nil
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/ -run TestHostSecret -v`
Expected: PASS (all subtests for both `sqlite` and `memory`).

Then the whole store package: `go test -tags "..." ./internal/store/` → PASS (no regressions).

- [ ] **Step 7: Commit**

```bash
git add internal/store/
git commit -m "feat(store): persist per-host secret values (sealed, keyed by host+name) (#54)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Service — persist on PUT, delete from store, provisionable helper

**Files:**
- Modify: `internal/instance/service.go` (`PutHostSecret` ~line 500, `DeleteHostSecret` ~line 514)
- Modify: `internal/instance/migrate.go` (add `hostSecretProvisionable` helper)
- Modify: `internal/api/secrets.go` (`putSecret` handler ~line 25)
- Test: `internal/instance/host_secret_provision_test.go` (create)

> **Caller note:** `Service.PutHostSecret` gains a `persist bool` parameter. After editing, grep for every caller and update it:
> `grep -rn "PutHostSecret(" internal/ | grep -v "store\."` — expect the API handler and any service tests. The store interface's `PutHostSecret` is a *different* method (3 args) — do not touch those calls.

- [ ] **Step 1: Write the failing test**

Create `internal/instance/host_secret_provision_test.go`:

```go
package instance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

func newHostSecretSvc(t *testing.T) (*Service, *fake.Fake, *store.Memory) {
	t.Helper()
	hosts := []config.Host{
		{ID: "h1", Addr: "unix", Socket: "/a"},
		{ID: "h2", Addr: "unix", Socket: "/b"},
	}
	f := fake.New()
	mem := store.NewMemory()
	svc := NewService(f, hosts, []config.Template{templateWithHostSecret()})
	svc.SetStore(mem)
	return svc, f, mem
}

func TestPutHostSecret_PersistsByDefault(t *testing.T) {
	svc, f, mem := newHostSecretSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v"), true))

	// Pushed to the host...
	_, err := f.SecretInspect(ctx, "h1", "shared-pull-token")
	require.NoError(t, err)
	// ...and persisted in the store.
	got, err := mem.GetHostSecret(ctx, "h1", "shared-pull-token")
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), got)
}

func TestPutHostSecret_PersistFalseSkipsStore(t *testing.T) {
	svc, f, mem := newHostSecretSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v"), false))

	_, err := f.SecretInspect(ctx, "h1", "shared-pull-token") // still on host
	require.NoError(t, err)
	_, err = mem.GetHostSecret(ctx, "h1", "shared-pull-token") // not in store
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestPutHostSecret_NoStoreIsNoOp(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/a"}}
	f := fake.New()
	svc := NewService(f, hosts, []config.Template{templateWithHostSecret()})
	// no SetStore
	ctx := context.Background()
	require.NoError(t, svc.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v"), true))
	_, err := f.SecretInspect(ctx, "h1", "shared-pull-token")
	require.NoError(t, err)
}

func TestDeleteHostSecret_RemovesFromStore(t *testing.T) {
	svc, _, mem := newHostSecretSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v"), true))
	require.NoError(t, svc.DeleteHostSecret(ctx, "h1", "shared-pull-token"))
	_, err := mem.GetHostSecret(ctx, "h1", "shared-pull-token")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestHostSecretProvisionable(t *testing.T) {
	svc, _, mem := newHostSecretSvc(t)
	ctx := context.Background()
	require.NoError(t, mem.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v")))

	ok, err := svc.hostSecretProvisionable(ctx, "h1", "shared-pull-token")
	require.NoError(t, err)
	assert.True(t, ok, "persisted for source host -> provisionable")

	ok, err = svc.hostSecretProvisionable(ctx, "h1", "absent")
	require.NoError(t, err)
	assert.False(t, ok, "not persisted -> not provisionable, not an error")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run "TestPutHostSecret|TestDeleteHostSecret_RemovesFromStore|TestHostSecretProvisionable" -v`
Expected: FAIL — `PutHostSecret` wants 5 args / `hostSecretProvisionable` undefined.

- [ ] **Step 3: Update `Service.PutHostSecret` and `DeleteHostSecret`**

In `internal/instance/service.go`, replace `PutHostSecret` and `DeleteHostSecret` with:

```go
// PutHostSecret creates-or-rotates a host secret on the host, then (when the
// store is enabled and persist is true) records the value so a later
// migrate/evacuate can re-provision it on a destination. We "rotate" by
// removing then recreating, since podman secrets are immutable. Push happens
// before persist: we never store a value we failed to apply to the host.
func (s *Service) PutHostSecret(ctx context.Context, host, name string, value []byte, persist bool) error {
	if _, ok := s.host(host); !ok {
		return ErrUnknownHost
	}
	if _, err := s.client.SecretInspect(ctx, host, name); err == nil {
		if err := s.client.SecretRemove(ctx, host, name); err != nil {
			return err
		}
	}
	if err := s.client.SecretCreate(ctx, host, name, wrapAsKubeSecret(name, value)); err != nil {
		return err
	}
	if s.store != nil && persist {
		if err := s.store.PutHostSecret(ctx, host, name, value); err != nil {
			return fmt.Errorf("persist host secret: %w", err)
		}
	}
	return nil
}

func (s *Service) DeleteHostSecret(ctx context.Context, host, name string) error {
	if _, ok := s.host(host); !ok {
		return ErrUnknownHost
	}
	if err := s.client.SecretRemove(ctx, host, name); err != nil && !errors.Is(err, podman.ErrNotFound) {
		return err
	}
	if s.store != nil {
		if err := s.store.DeleteHostSecret(ctx, host, name); err != nil {
			return fmt.Errorf("delete persisted host secret: %w", err)
		}
	}
	return nil
}
```

(`fmt`, `errors`, `podman` already imported in `service.go`.)

- [ ] **Step 4: Add the `hostSecretProvisionable` helper**

In `internal/instance/migrate.go`, add (place just above `preflightIssues`):

```go
// hostSecretProvisionable reports whether per-host secret `name` — already known
// absent on the destination — can be auto-provisioned from the source host's
// persisted value. A non-nil error is an infra/store failure the caller should
// treat as inconclusive. Returns (false, nil) when the store is disabled or holds
// no value (i.e. genuinely missing, not an error).
func (s *Service) hostSecretProvisionable(ctx context.Context, fromHost, name string) (bool, error) {
	if s.store == nil {
		return false, nil
	}
	switch _, err := s.store.GetHostSecret(ctx, fromHost, name); {
	case err == nil:
		return true, nil
	case errors.Is(err, store.ErrNotFound):
		return false, nil
	default:
		return false, err
	}
}
```

Ensure `migrate.go` imports `"github.com/iotready/podman-api/internal/store"` (it references `s.store`; if the import is not yet present, add it).

- [ ] **Step 5: Update the API handler to decode `persist`**

In `internal/api/secrets.go`, change the `putSecret` body struct and call:

```go
	var body struct {
		Value   string `json:"value"`
		Persist *bool  `json:"persist"` // optional; defaults to true
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if body.Value == "" {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "value is required"})
		return
	}
	persist := true
	if body.Persist != nil {
		persist = *body.Persist
	}
	if err := h.svc.PutHostSecret(r.Context(), host, name, []byte(body.Value), persist); err != nil {
		WriteError(w, err)
		return
	}
```

- [ ] **Step 6: Fix any other callers, then run tests**

Run: `grep -rn "\.PutHostSecret(" internal/ | grep -v "store\."` — update any test that calls the service method with the old 4-arg form to pass a trailing `true`.

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ ./internal/api/ -v -run "HostSecret|putSecret|Secret"`
Expected: PASS (new tests green; existing secret tests still green).

- [ ] **Step 7: Commit**

```bash
git add internal/instance/service.go internal/instance/migrate.go internal/api/secrets.go internal/instance/host_secret_provision_test.go
git commit -m "feat(instance): persist host secrets on PUT; provisionable helper (#54)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Preflight — treat provisionable per-host secrets as non-blocking

**Files:**
- Modify: `internal/instance/migrate.go` (`preflightIssues` ~lines 123-169; `preflightDest` ~lines 175-180)
- Modify: `internal/instance/evacuate_plan_test.go` (one caller of `preflightIssues`, line ~57)
- Test: `internal/instance/host_secret_provision_test.go` (append)

> `preflightIssues` currently returns `[]error`. It will now return `([]error, []string)` — the second value is the per-host secrets that are absent-on-dest-but-provisionable. Both callers (`preflightDest`, `planMove`) and the existing test `TestPreflightIssues_CollectsAll` must be updated.

- [ ] **Step 1: Write the failing test**

Append to `internal/instance/host_secret_provision_test.go`:

```go
func TestPreflightIssues_ProvisionableNotBlocking(t *testing.T) {
	svc, _, mem := newHostSecretSvc(t)
	ctx := context.Background()
	// shared-pull-token absent on dest h2 BUT persisted for source h1.
	require.NoError(t, mem.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v")))

	tmpl := templateWithHostSecret()
	eff := map[string]any{"slug": "s1", "image": "img"}
	req := MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "needs-host-secret", Slug: "s1"}
	issues, provisionable := svc.preflightIssues(ctx, req, tmpl, eff)

	assert.Empty(t, issues, "provisionable secret must not be a blocking issue")
	assert.Equal(t, []string{"shared-pull-token"}, provisionable)
}

func TestPreflightIssues_NotPersistedStillBlocks(t *testing.T) {
	svc, _, _ := newHostSecretSvc(t)
	ctx := context.Background()
	// Nothing persisted: absent on dest AND not provisionable -> blocking.
	tmpl := templateWithHostSecret()
	eff := map[string]any{"slug": "s1", "image": "img"}
	req := MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "needs-host-secret", Slug: "s1"}
	issues, provisionable := svc.preflightIssues(ctx, req, tmpl, eff)

	require.Len(t, issues, 1)
	assert.ErrorIs(t, issues[0], ErrHostSecretMissing)
	assert.Empty(t, provisionable)
}

func TestPreflightIssues_PresentOnDestNotProvisioned(t *testing.T) {
	svc, f, mem := newHostSecretSvc(t)
	ctx := context.Background()
	// Already present on the destination AND persisted: present wins, no provision.
	require.NoError(t, f.SecretCreate(ctx, "h2", "shared-pull-token", []byte("x")))
	require.NoError(t, mem.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v")))

	tmpl := templateWithHostSecret()
	eff := map[string]any{"slug": "s1", "image": "img"}
	req := MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "needs-host-secret", Slug: "s1"}
	issues, provisionable := svc.preflightIssues(ctx, req, tmpl, eff)

	assert.Empty(t, issues)
	assert.Empty(t, provisionable, "present-on-dest secret is not in the provision list")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestPreflightIssues -v`
Expected: FAIL — `preflightIssues` returns 1 value, tests expect 2 (compile error).

- [ ] **Step 3: Change `preflightIssues` to return provisionable**

In `internal/instance/migrate.go`, update the signature, the doc comment, and EVERY return. The full new body:

```go
// preflightIssues runs every destination preflight check and returns (issues,
// provisionable): all problems found in check order, plus the per-host secrets
// that are absent on the destination but can be auto-provisioned from the source
// host's persisted value. A nil/empty issues slice means the destination would
// accept the instance (after provisioning any returned secrets). Each issue is a
// sentinel-wrapped blocking condition or an infrastructure error that made a
// check inconclusive. preflightDest (executor), migratePostStop (executor), and
// PlanEvacuation (preview) all build on this, so they never disagree.
func (s *Service) preflightIssues(ctx context.Context, req MigrateRequest, tmpl config.Template, eff map[string]any) ([]error, []string) {
	var issues []error
	var provisionable []string
	hostCfg, _ := s.host(req.ToHost)
	if hostCfg.Drain {
		issues = append(issues, ErrHostDraining)
	}
	if _, err := s.client.PodInspect(ctx, req.ToHost, podName(req.Template, req.Slug)); err == nil {
		issues = append(issues, ErrInstanceExists)
	} else if !errors.Is(err, podman.ErrNotFound) {
		return append(issues, fmt.Errorf("inspect dest pod: %w", err)), provisionable
	}
	for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
		_, err := s.client.SecretInspect(ctx, req.ToHost, name)
		if err == nil {
			continue // present on dest
		}
		if !errors.Is(err, podman.ErrNotFound) {
			// Infra error (host unreachable): report and stop — avoids piling up
			// timeout-length RPCs on the executor's failure path.
			return append(issues, fmt.Errorf("inspect host secret %q: %w", name, err)), provisionable
		}
		// Absent on dest: provisionable from the source host's persisted value?
		ok, perr := s.hostSecretProvisionable(ctx, req.FromHost, name)
		if perr != nil {
			return append(issues, fmt.Errorf("lookup persisted host secret %q: %w", name, perr)), provisionable
		}
		if ok {
			provisionable = append(provisionable, name)
			continue
		}
		issues = append(issues, fmt.Errorf("%w: %s", ErrHostSecretMissing, name))
	}
	want, err := s.requiredHostPorts(tmpl, eff)
	if err != nil {
		return append(issues, err), provisionable
	}
	if len(want) > 0 {
		used, err := s.PortsInUse(ctx, req.ToHost)
		if err != nil {
			return append(issues, fmt.Errorf("ports in use: %w", err)), provisionable
		}
		busy := map[int]bool{}
		for _, p := range used {
			busy[p.HostPort] = true
		}
		for _, p := range want {
			if busy[p] {
				issues = append(issues, fmt.Errorf("%w: %d", ErrPortConflict, p))
			}
		}
	}
	return issues, provisionable
}
```

- [ ] **Step 4: Update `preflightDest`**

In `internal/instance/migrate.go`, change `preflightDest`:

```go
func (s *Service) preflightDest(ctx context.Context, req MigrateRequest, tmpl config.Template, eff map[string]any) error {
	if errs, _ := s.preflightIssues(ctx, req, tmpl, eff); len(errs) > 0 {
		return errs[0]
	}
	return nil
}
```

- [ ] **Step 5: Fix the existing `preflightIssues` caller in tests**

In `internal/instance/evacuate_plan_test.go`, `TestPreflightIssues_CollectsAll` (line ~57): change

```go
	errs := svc.preflightIssues(ctx, MigrateRequest{...}, tmpl, eff)
```
to
```go
	errs, _ := svc.preflightIssues(ctx, MigrateRequest{...}, tmpl, eff)
```

(`planMove` in `evacuate_plan.go` is updated in Task 5; until then it will not compile — that is expected. To keep this task's package compiling for its own test run, also apply the Task 5 Step 3 one-liner change to `planMove` now: change `for _, e := range s.preflightIssues(ctx, m, tmpl, eff) {` to `errs, _ := s.preflightIssues(ctx, m, tmpl, eff); for _, e := range errs {`. Task 5 then builds on that.)

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run "TestPreflightIssues|TestPlanEvacuation" -v`
Expected: PASS (new provisionable tests + existing preflight/plan tests).

- [ ] **Step 7: Commit**

```bash
git add internal/instance/migrate.go internal/instance/evacuate_plan.go internal/instance/evacuate_plan_test.go internal/instance/host_secret_provision_test.go
git commit -m "feat(instance): preflight treats provisionable host secrets as non-blocking (#54)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Migrate executor — provision missing host secrets on the destination

**Files:**
- Modify: `internal/instance/migrate.go` (`Migrate` call site ~line 227; `migratePostStop` signature + body ~line 255)
- Test: `internal/instance/host_secret_provision_test.go` (append)

- [ ] **Step 1: Write the failing test**

Append to `internal/instance/host_secret_provision_test.go`. These drive a full `Migrate`; model the setup on existing migrate tests (`migrate_test.go`). `templateWithHostSecret` has no volumes and a single container, so a successful `Migrate` needs the source pod present/running and the dest reachable.

Add `"github.com/iotready/podman-api/internal/podman"` to this test file's import block (Task 2's tests did not need it; Task 4 does).

```go
// seedHostSecretInstance puts a running instance of needs-host-secret on h1 with
// a stored spec, so Migrate can load + move it. Mirrors TestMigrate_HappyPath:
// a Status:"Running" source pod is all the fake needs (this template has no
// volumes and no healthcheck, so waitRunning is liveness-gated).
func seedHostSecretInstance(t *testing.T, f *fake.Fake, mem *store.Memory, slug string) {
	t.Helper()
	require.NoError(t, mem.PutSpec(context.Background(), store.Spec{
		Host: "h1", Template: "needs-host-secret", Slug: slug,
		Parameters: map[string]any{"slug": slug, "image": "img"},
		Secrets:    map[string]string{},
	}))
	f.AddPod("h1", podman.Pod{Name: "needs-host-secret-" + slug, Status: "Running"})
}

func TestMigrate_ProvisionsPersistedHostSecret(t *testing.T) {
	svc, f, mem := newHostSecretSvc(t)
	ctx := context.Background()
	seedHostSecretInstance(t, f, mem, "s1")
	require.NoError(t, mem.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("topsecret")))

	err := svc.Migrate(ctx, MigrateRequest{
		FromHost: "h1", ToHost: "h2", Template: "needs-host-secret", Slug: "s1",
	}, nil)
	require.NoError(t, err)

	// The per-host secret now exists on the destination.
	_, err = f.SecretInspect(ctx, "h2", "shared-pull-token")
	assert.NoError(t, err, "host secret must be provisioned on the destination")
}

func TestMigrate_MissingUnpersistedHostSecretFails(t *testing.T) {
	svc, f, mem := newHostSecretSvc(t)
	ctx := context.Background()
	seedHostSecretInstance(t, f, mem, "s1")
	// Nothing persisted -> preflight blocks before the source is touched.

	err := svc.Migrate(ctx, MigrateRequest{
		FromHost: "h1", ToHost: "h2", Template: "needs-host-secret", Slug: "s1",
	}, nil)
	assert.ErrorIs(t, err, ErrHostSecretMissing)

	// Source instance untouched (preflight failed before Stop).
	p, ierr := f.PodInspect(ctx, "h1", "needs-host-secret-s1")
	require.NoError(t, ierr)
	assert.Equal(t, "Running", p.Status)
}
```

> The fixture mirrors `TestMigrate_HappyPath` (`migrate_test.go:186`). If a future change makes `waitRunning` stricter, copy that test's exact source/dest setup. The assertions are the point: persisted ⇒ provisioned on dest; unpersisted ⇒ `ErrHostSecretMissing` with the source pod still `Running`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestMigrate_ProvisionsPersistedHostSecret -v`
Expected: FAIL — the dest secret is not created (provisioning not implemented yet).

- [ ] **Step 3: Pass `tmpl` into `migratePostStop` and add the provisioning loop**

In `internal/instance/migrate.go`, change the `Migrate` call site (~line 227):

```go
	if err := s.migratePostStop(ctx, req, eff, tmpl, spec.Secrets, step); err != nil {
```

Change the `migratePostStop` signature and prepend the provisioning loop as its first action (before listing/copying volumes):

```go
func (s *Service) migratePostStop(ctx context.Context, req MigrateRequest, eff map[string]any, tmpl config.Template, secrets map[string]string, step func(step, detail string)) error {
	// Provision any persisted per-host secrets the destination is missing, from
	// the source host's stored value. Idempotent: only creates what is absent.
	// Provisioned secrets are intentionally left in place on rollback — they are
	// shared, host-scoped, and additive, and other instances on the destination
	// may rely on them (Delete's PruneSecrets only reaps per-instance secrets).
	for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
		if _, err := s.client.SecretInspect(ctx, req.ToHost, name); err == nil {
			continue // already present on dest
		}
		val, err := s.store.GetHostSecret(ctx, req.FromHost, name)
		if errors.Is(err, store.ErrNotFound) {
			continue // not provisionable; Apply's own pre-check will reject it
		}
		if err != nil {
			return fmt.Errorf("load host secret %q: %w", name, err)
		}
		if err := s.client.SecretCreate(ctx, req.ToHost, name, wrapAsKubeSecret(name, val)); err != nil {
			// A concurrent move may have created it between inspect and create;
			// tolerate that, fail on anything else.
			if _, ie := s.client.SecretInspect(ctx, req.ToHost, name); ie == nil {
				continue
			}
			return fmt.Errorf("provision host secret %q: %w", name, err)
		}
		step("provision-secret", name)
	}

	vols, err := s.InstanceVolumes(ctx, req.FromHost, req.Template, req.Slug)
	// ... rest of the existing body unchanged ...
```

(The existing volume-copy / `Apply` / `waitRunning` code stays as-is below the new loop.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run "TestMigrate" -v`
Expected: PASS — provisioning test green; missing-unpersisted test green; all pre-existing migrate tests still green.

- [ ] **Step 5: Commit**

```bash
git add internal/instance/migrate.go internal/instance/host_secret_provision_test.go
git commit -m "feat(instance): provision persisted host secrets on migrate destination (#54)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Plan-preview — report `provisions` per move

**Files:**
- Modify: `internal/instance/evacuate_plan.go` (`PlannedMove` struct; `planMove`)
- Test: `internal/instance/evacuate_plan_test.go` (append)

- [ ] **Step 1: Write the failing test**

Append to `internal/instance/evacuate_plan_test.go`:

```go
func TestPlanEvacuation_ProvisionsPersistedHostSecret(t *testing.T) {
	svc, _, mem := newPlanSvc(t)
	ctx := context.Background()
	// Instance referencing a per-host secret; the secret is persisted for source h1
	// but absent on dest h2 -> non-blocking, reported in provisions.
	seedSpec(t, mem, "h1", "needs-host-secret", "s1", map[string]any{"slug": "s1", "image": "x"})
	require.NoError(t, mem.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v")))

	plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"s1": "h2"}})
	require.NoError(t, err)
	require.Len(t, plan.Moves, 1)
	assert.True(t, plan.Moves[0].OK, "provisionable secret keeps the move ok")
	assert.Empty(t, plan.Moves[0].Issues)
	assert.Equal(t, []string{"shared-pull-token"}, plan.Moves[0].Provisions)
}

func TestPlanEvacuation_ProvisionsEmptyByDefault(t *testing.T) {
	svc, _, mem := newPlanSvc(t)
	ctx := context.Background()
	seedPGSpec(t, mem, "db1")
	plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"db1": "h2"}})
	require.NoError(t, err)
	require.Len(t, plan.Moves, 1)
	assert.NotNil(t, plan.Moves[0].Provisions, "provisions serializes as [], never null")
	assert.Empty(t, plan.Moves[0].Provisions)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run "TestPlanEvacuation_Provisions" -v`
Expected: FAIL — `Provisions` field undefined on `PlannedMove`.

- [ ] **Step 3: Add the `Provisions` field and populate it**

In `internal/instance/evacuate_plan.go`, add the field to `PlannedMove`:

```go
type PlannedMove struct {
	Slug       string      `json:"slug"`
	Template   string      `json:"template"`
	ToHost     string      `json:"to_host"`
	OK         bool        `json:"ok"`     // true iff Issues is empty
	Issues     []PlanIssue `json:"issues"`
	Provisions []string    `json:"provisions"` // per-host secrets to be created on dest; [] not null
}
```

In `planMove`, initialize `Provisions` and consume both return values of `preflightIssues`. The init line:

```go
	pm := PlannedMove{Slug: m.Slug, Template: m.Template, ToHost: m.ToHost, Issues: []PlanIssue{}, Provisions: []string{}}
```

Replace the preflight loop:

```go
	errs, provisionable := s.preflightIssues(ctx, m, tmpl, eff)
	for _, e := range errs {
		pm.Issues = append(pm.Issues, classifyPlanIssue(e))
	}
	pm.Provisions = append(pm.Provisions, provisionable...)
	pm.OK = len(pm.Issues) == 0
	return pm
```

(`OK` ignores `Provisions` by design — a provisionable secret is non-blocking.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run "TestPlanEvacuation" -v`
Expected: PASS (new provisions tests + all existing plan tests).

- [ ] **Step 5: Commit**

```bash
git add internal/instance/evacuate_plan.go internal/instance/evacuate_plan_test.go
git commit -m "feat(instance): evacuate plan-preview reports provisionable host secrets (#54)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: OpenAPI + API tests + full green

**Files:**
- Modify: `api/openapi.yaml` (PUT host-secret request body; `PlannedMove` schema)
- Modify: `internal/api/secrets_test.go` or `internal/api/coverage_more_test.go` (persist decode); `internal/api/evacuate_test.go` (provisions surfaced)
- Test: run the whole suite

- [ ] **Step 1: Write the failing API tests**

Two API-level checks. The secrets harness `newSrvWithSecrets` wires a store-less service, so the API test confirms the handler *accepts and decodes* `persist` (→ 204); the store-effect of `persist` is already covered at the service level in Task 2. The plan check confirms `provisions` serializes (as `[]` here, since `migrateTmpl` declares no per-host secret).

Append to `internal/api/secrets_test.go` (imports `bytes`, `net/http`, `testing`, `require`/`assert` already used there):

```go
// The handler accepts the optional "persist" field (default true) and returns 204.
func TestPutSecret_PersistField(t *testing.T) {
	srv, tok := newSrvWithSecrets(t)
	for _, body := range []string{`{"value":"v"}`, `{"value":"v","persist":false}`, `{"value":"v","persist":true}`} {
		req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/secrets/s1", bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, http.StatusNoContent, resp.StatusCode, "body %s", body)
	}
}
```

In `internal/api/evacuate_test.go`, add `Provisions` to the `planResp.Moves` struct so the decoder can see it:

```go
	Moves    []struct {
		Slug   string `json:"slug"`
		ToHost string `json:"to_host"`
		OK     bool   `json:"ok"`
		Issues []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"issues"`
		Provisions []string `json:"provisions"`
	} `json:"moves"`
```

Then append the serialization test:

```go
// provisions is always present in the response (empty when nothing to provision).
func TestEvacuatePlan_API_ProvisionsSerialized(t *testing.T) {
	srv, tok, mem, _ := newEvacSrv(t)
	seedEvacWithParams(t, mem, "db1")
	resp := postEvacuatePlan(t, srv, tok, instance.EvacuateRequest{
		FromHost: "h1", Map: map[string]string{"db1": "h2"},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out planResp
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Moves, 1)
	assert.NotNil(t, out.Moves[0].Provisions, "provisions must serialize as [], not null")
	assert.Empty(t, out.Moves[0].Provisions)
}
```

- [ ] **Step 2: Run tests to verify they fail / pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/ -run "TestPutSecret_PersistField|TestEvacuatePlan_API_ProvisionsSerialized" -v`
Expected: with Tasks 2 & 5 already merged, these should PASS immediately (the handler decodes `persist`; the plan emits `provisions`). If `TestPutSecret_PersistField` fails to compile/return 204, the handler change from Task 2 Step 5 is missing — fix it. If `provisions` is `null`, the `Provisions: []string{}` init from Task 5 Step 3 is missing — fix it.

- [ ] **Step 3: (production code already in place)**

No new production code: Task 2 added `persist` decoding, Task 5 added the `provisions` field with a non-nil init. This task only adds the API-level tests + OpenAPI. If a test reveals a gap, fix it in the owning file.

- [ ] **Step 4: Update `api/openapi.yaml`**

Find the PUT host-secret operation (path `/hosts/{host}/secrets/{name}`) and add `persist` to its request body schema:

```yaml
        persist:
          type: boolean
          default: true
          description: >
            When the state store is enabled, also persist this value so a later
            migrate/evacuate can auto-provision the secret on a destination host.
            Set false to push to the host only. No effect when the store is disabled.
```

Find the `PlannedMove` schema and add:

```yaml
        provisions:
          type: array
          items:
            type: string
          description: >
            Per-host secrets absent on the destination that the evacuate will
            auto-provision from the source host's persisted value. Non-blocking:
            a move with provisions and no issues is still ok.
```

If `internal/api/openapi_test.go` validates property names against the served schema, add `provisions`/`persist` there as needed (run it to see).

- [ ] **Step 5: Full suite, gofmt, vet**

```bash
gofmt -l .            # must print nothing
go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./...
make test
```
Expected: gofmt clean, vet clean, all packages `ok`.

- [ ] **Step 6: Commit**

```bash
git add internal/api/ api/openapi.yaml
git commit -m "feat(api): persist flag on PUT host-secret; provisions in evacuate plan; openapi (#54)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification (after all tasks)

- [ ] `make test` fully green; `gofmt -l .` empty; `go vet` clean.
- [ ] Spec coverage re-check against `2026-06-05-host-secret-provisioning-design.md`:
  - Persist-by-default on PUT, opt-out via `persist:false` ✔ (Task 2, 6)
  - Per-host keying, provision from source host's value ✔ (Task 1, 4)
  - Preflight non-blocking for provisionable; blocking for unpersisted ✔ (Task 3)
  - Provision before Apply; rollback leaves secrets in place (free via prune scope) ✔ (Task 4)
  - Plan-preview `provisions` list, `ok` unaffected, `[]` not null ✔ (Task 5)
  - Store-disabled path unchanged; concurrent-create race benign ✔ (Task 2, 4)
- [ ] Wiki (separate repo, **post-merge**): Operating — "Previewing an evacuate" (`provisions`) + host-secrets section (persist-by-default, `persist:false`, auto-provision, rollback-leaves-in-place, store-required).
