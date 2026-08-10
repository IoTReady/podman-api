# Host Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `POST /hosts/{host}/rename` so a managed host's id can be changed atomically — store rows migrated, config file rewritten, new id live immediately, no restart.

**Architecture:** A `store.RenameHost` transaction migrates `specs`/`host_secrets`/`backups` rows (refusing if the host has any backups, since S3 blob re-keying is out of scope). A pure `config.RenameHostFile` function rewrites the one `id:` line in the host's YAML file, byte-for-byte otherwise, returning the original bytes for revert-on-failure. `instance.Service.RenameHost` orchestrates store + in-memory `SetHosts` swap. A new API handler wires validation, the file rewrite, and the service call together, reverting the file if the service call fails.

**Tech Stack:** Go, `database/sql` (`modernc.org/sqlite`), stdlib `net/http`, `testify` for assertions.

## Global Constraints

- Repo: `~/projects/podman-api` (OSS core). Work happens on a feature branch in a worktree under `.worktrees/` per `CLAUDE.md:146`.
- `docs/superpowers/` is gitignored in this repo but existing spec/plan docs are force-added (`git add -f`) — do the same for any new doc file.
- Follow the existing `<resource>:write` auth-scope convention (`instances:write`, `templates:write`, `secrets:write`) — this feature adds `hosts:write`.
- v1 explicitly refuses to rename a host with any backup rows (S3 blob re-keying is a documented follow-up, not built here).
- Design doc: `docs/superpowers/specs/2026-08-10-host-rename-design.md` — consult it for the "why" behind any constraint below.

---

### Task 1: `store.RenameHost` — SQLite + Memory + interface + errors

**Files:**
- Modify: `internal/store/store.go` (add sentinel errors + interface method)
- Modify: `internal/store/sqlite.go` (implementation)
- Modify: `internal/store/memory.go` (implementation)
- Test: `internal/store/sqlite_test.go` (new test functions)
- Test: `internal/store/memory_test.go` (new test functions)

**Interfaces:**
- Produces: `store.ErrHostRenameConflict` (sentinel error — `newID` already has spec rows), `store.ErrHostRenameHasBackups` (sentinel error — `oldID` has backup rows), and a new `Store` interface method `RenameHost(ctx context.Context, oldID, newID string) error`, implemented by both `*SQLite` and `*Memory`.

- [ ] **Step 1: Add the two sentinel errors to `internal/store/store.go`**

Insert after the existing `ErrSecretsUndecryptable` var block (around line 34, right before the `InjectorSecret` type):

```go
// ErrHostRenameConflict is returned by RenameHost when newID already has spec
// rows — renaming into an id that already has state would silently merge two
// hosts' instances.
var ErrHostRenameConflict = errors.New("store: rename target host id already has state")

// ErrHostRenameHasBackups is returned by RenameHost when oldID has any backup
// rows. On-demand snapshot backups are keyed by S3 blob prefixes that are NOT
// re-keyed by this call (out of scope — see docs/superpowers/specs/2026-08-10-host-rename-design.md),
// so renaming a host with backups would make those backups unreachable via the
// restore API. Refuse rather than silently orphan them.
var ErrHostRenameHasBackups = errors.New("store: host has backups; rename would orphan their blob storage")
```

- [ ] **Step 2: Add `RenameHost` to the `Store` interface**

In `internal/store/store.go`, add the method to the interface, right before its closing `SecretsEnabled() bool` line (end of the interface, around line 97):

```go
	// RenameHost atomically migrates every specs/host_secrets/backups row from
	// oldID to newID. Returns ErrHostRenameConflict if newID already has spec
	// rows, or ErrHostRenameHasBackups if oldID has any backup rows.
	RenameHost(ctx context.Context, oldID, newID string) error

	// SecretsEnabled reports whether this store can persist secrets — true only
```

(Keep the existing `SecretsEnabled` doc comment and signature immediately after — you're inserting `RenameHost` as a new interface method, not replacing anything.)

- [ ] **Step 3: Write the failing SQLite test**

Add to `internal/store/sqlite_test.go` (check the top of that file for the existing `newTestSQLite(t)` — or equivalent — helper used by other tests in the file, and reuse it):

```go
func TestSQLiteRenameHost(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()

	require.NoError(t, s.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}, Domains: []string{}}))
	require.NoError(t, s.PutHostSecret(ctx, "h1", "tok", []byte("secret")))

	require.NoError(t, s.RenameHost(ctx, "h1", "h2"))

	_, err := s.GetSpec(ctx, "h1", "app", "a")
	require.ErrorIs(t, err, ErrNotFound)
	got, err := s.GetSpec(ctx, "h2", "app", "a")
	require.NoError(t, err)
	require.Equal(t, "h2", got.Host)

	_, err = s.GetHostSecret(ctx, "h1", "tok")
	require.ErrorIs(t, err, ErrNotFound)
	val, err := s.GetHostSecret(ctx, "h2", "tok")
	require.NoError(t, err)
	require.Equal(t, []byte("secret"), val)
}

func TestSQLiteRenameHostConflict(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()

	require.NoError(t, s.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}, Domains: []string{}}))
	require.NoError(t, s.PutSpec(ctx, Spec{Host: "h2", Template: "app", Slug: "b", Parameters: map[string]any{}, Domains: []string{}}))

	err := s.RenameHost(ctx, "h1", "h2")
	require.ErrorIs(t, err, ErrHostRenameConflict)

	// h1's row must be untouched.
	_, err = s.GetSpec(ctx, "h1", "app", "a")
	require.NoError(t, err)
}

func TestSQLiteRenameHostHasBackups(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()

	require.NoError(t, s.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}, Domains: []string{}}))
	require.NoError(t, s.CreateBackup(ctx, Backup{ID: "b1", Host: "h1", Template: "app", Slug: "a", State: BackupStateCreating, Created: time.Now()}))

	err := s.RenameHost(ctx, "h1", "h2")
	require.ErrorIs(t, err, ErrHostRenameHasBackups)

	_, err = s.GetSpec(ctx, "h1", "app", "a")
	require.NoError(t, err) // untouched
}
```

Check `internal/store/backups.go` for the exact `BackupState` constant name for "creating" (grep `BackupState` in that file) before using it — use whatever the real constant is named if it differs from `BackupStateCreating`.

- [ ] **Step 4: Run the tests to verify they fail with "undefined: RenameHost" / "undefined: ErrHostRenameConflict" / "undefined: ErrHostRenameHasBackups"**

Run: `cd ~/projects/podman-api && go test ./internal/store/... -run TestSQLiteRenameHost -v`
Expected: compile failure referencing the missing method/errors.

- [ ] **Step 5: Implement `RenameHost` on `*SQLite`**

Add to `internal/store/sqlite.go`, after `PruneJobs` (around line 989, right before the `// TemplateStore implementation` section marker):

```go
// RenameHost atomically migrates every specs/host_secrets/backups row from
// oldID to newID in one transaction. See ErrHostRenameConflict and
// ErrHostRenameHasBackups for the two refusal cases.
func (s *SQLite) RenameHost(ctx context.Context, oldID, newID string) error {
	return s.write(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit

		var conflictCount int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM specs WHERE host = ?`, newID).Scan(&conflictCount); err != nil {
			return err
		}
		if conflictCount > 0 {
			return ErrHostRenameConflict
		}

		var backupCount int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM backups WHERE host = ?`, oldID).Scan(&backupCount); err != nil {
			return err
		}
		if backupCount > 0 {
			return ErrHostRenameHasBackups
		}

		if _, err := tx.ExecContext(ctx, `UPDATE specs SET host = ? WHERE host = ?`, newID, oldID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE host_secrets SET host = ? WHERE host = ?`, newID, oldID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE backups SET host = ? WHERE host = ?`, newID, oldID); err != nil {
			return err
		}

		return tx.Commit()
	})
}
```

- [ ] **Step 6: Run the SQLite tests to verify they pass**

Run: `cd ~/projects/podman-api && go test ./internal/store/... -run TestSQLiteRenameHost -v`
Expected: PASS (all three tests)

- [ ] **Step 7: Write the failing Memory test**

Add to `internal/store/memory_test.go`:

```go
func TestMemoryRenameHost(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	require.NoError(t, m.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}}))
	require.NoError(t, m.PutHostSecret(ctx, "h1", "tok", []byte("secret")))

	require.NoError(t, m.RenameHost(ctx, "h1", "h2"))

	_, err := m.GetSpec(ctx, "h1", "app", "a")
	require.ErrorIs(t, err, ErrNotFound)
	got, err := m.GetSpec(ctx, "h2", "app", "a")
	require.NoError(t, err)
	require.Equal(t, "h2", got.Host)

	_, err = m.GetHostSecret(ctx, "h1", "tok")
	require.ErrorIs(t, err, ErrNotFound)
	val, err := m.GetHostSecret(ctx, "h2", "tok")
	require.NoError(t, err)
	require.Equal(t, []byte("secret"), val)
}

func TestMemoryRenameHostConflict(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	require.NoError(t, m.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}}))
	require.NoError(t, m.PutSpec(ctx, Spec{Host: "h2", Template: "app", Slug: "b", Parameters: map[string]any{}}))

	require.ErrorIs(t, m.RenameHost(ctx, "h1", "h2"), ErrHostRenameConflict)
}

func TestMemoryRenameHostHasBackups(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	require.NoError(t, m.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}}))
	require.NoError(t, m.CreateBackup(ctx, Backup{ID: "b1", Host: "h1", Template: "app", Slug: "a", State: BackupStateCreating, Created: time.Now()}))

	require.ErrorIs(t, m.RenameHost(ctx, "h1", "h2"), ErrHostRenameHasBackups)
}
```

Use the same real `BackupState` constant name confirmed in Step 3.

- [ ] **Step 8: Run the Memory tests to verify they fail to compile**

Run: `cd ~/projects/podman-api && go test ./internal/store/... -run TestMemoryRenameHost -v`
Expected: compile failure, `RenameHost` undefined on `*Memory`.

- [ ] **Step 9: Implement `RenameHost` on `*Memory`**

First check `internal/store/memory.go` for how `hostSecrets` keys are split elsewhere (grep `"\x00"` in that file, e.g. inside `GetHostSecret`/`DeleteHostSecret`) and match that exact splitting approach (likely `strings.Cut(key, "\x00")`). Then add to `internal/store/memory.go`, after `DeleteBackup` (end of the `BackupStore` implementation block):

```go
// RenameHost migrates every spec/host-secret/backup entry from oldID to
// newID. Mirrors *SQLite's refusal cases: ErrHostRenameConflict if newID
// already has specs, ErrHostRenameHasBackups if oldID has any backups.
func (m *Memory) RenameHost(_ context.Context, oldID, newID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, s := range m.specs {
		if s.Host == newID {
			return ErrHostRenameConflict
		}
	}
	for _, b := range m.backups {
		if b.Host == oldID {
			return ErrHostRenameHasBackups
		}
	}

	for key, s := range m.specs {
		if s.Host != oldID {
			continue
		}
		delete(m.specs, key)
		s.Host = newID
		m.specs[memKey(newID, s.Template, s.Slug)] = s
	}
	for key, v := range m.hostSecrets {
		host, name, ok := strings.Cut(key, "\x00")
		if !ok || host != oldID {
			continue
		}
		delete(m.hostSecrets, key)
		m.hostSecrets[newID+"\x00"+name] = v
	}
	for id, b := range m.backups {
		if b.Host != oldID {
			continue
		}
		b.Host = newID
		m.backups[id] = b
	}
	return nil
}
```

`strings` is already imported in `memory.go` (used by other files in the package at minimum — confirm with the file's import block; add `"strings"` to the import list if it's not already there).

- [ ] **Step 10: Run the Memory tests to verify they pass**

Run: `cd ~/projects/podman-api && go test ./internal/store/... -run TestMemoryRenameHost -v`
Expected: PASS (all three tests)

- [ ] **Step 11: Run the full store package test suite**

Run: `cd ~/projects/podman-api && go test ./internal/store/... -v 2>&1 | tail -60`
Expected: PASS, no regressions.

- [ ] **Step 12: Commit**

```bash
cd ~/projects/podman-api
git add internal/store/store.go internal/store/sqlite.go internal/store/memory.go internal/store/sqlite_test.go internal/store/memory_test.go
git commit -m "feat(store): add RenameHost migrating specs/host_secrets/backups"
```

---

### Task 2: `config.RenameHostFile`

**Files:**
- Modify: `internal/config/hosts.go`
- Test: `internal/config/hosts_test.go`

**Interfaces:**
- Produces: `config.RenameHostFile(dir, oldID, newID string) (path string, original []byte, err error)` and `config.HostsDir` (a `string`-based type with a `RenameHostFile(oldID, newID string) (string, []byte, error)` method), for use as `internal/api`'s `HostFileRenamer` (Task 4) via duck typing — no import of `internal/api` needed here.

- [ ] **Step 1: Write the failing tests**

Add to `internal/config/hosts_test.go` (check the file's existing imports/helpers first — it likely already uses `os.MkdirTemp`/`t.TempDir()` patterns from testing `LoadHosts`; reuse `t.TempDir()`):

```go
func TestRenameHostFile(t *testing.T) {
	dir := t.TempDir()
	content := "id: h1\naddr: unix\nsocket: /run/podman.sock\n# a comment to preserve\nlabels:\n  env: dev\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "h1.yaml"), []byte(content), 0o644))

	path, original, err := RenameHostFile(dir, "h1", "h2")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, "h1.yaml"), path)
	require.Equal(t, []byte(content), original)

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "id: h2\naddr: unix\nsocket: /run/podman.sock\n# a comment to preserve\nlabels:\n  env: dev\n", string(got))

	hosts, err := LoadHosts(dir)
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	require.Equal(t, "h2", hosts[0].ID)
}

func TestRenameHostFileNoMatch(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "h1.yaml"), []byte("id: h1\naddr: unix\nsocket: /x\n"), 0o644))

	_, _, err := RenameHostFile(dir, "does-not-exist", "h2")
	require.Error(t, err)
}

func TestRenameHostFileUnexpectedIDLineFormat(t *testing.T) {
	dir := t.TempDir()
	// id on the same line as a trailing comment — the exact-line match must
	// fail closed rather than guess.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "h1.yaml"), []byte("id: h1 # primary\naddr: unix\nsocket: /x\n"), 0o644))

	_, _, err := RenameHostFile(dir, "h1", "h2")
	require.Error(t, err)
}

func TestHostsDirRenameHostFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "h1.yaml"), []byte("id: h1\naddr: unix\nsocket: /x\n"), 0o644))

	d := HostsDir(dir)
	path, original, err := d.RenameHostFile("h1", "h2")
	require.NoError(t, err)
	require.NotEmpty(t, path)
	require.Contains(t, string(original), "id: h1")
}
```

Add `"os"` and `"path/filepath"` to the test file's imports if not already present (they will be, since `LoadHosts` tests already exercise a temp dir).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ~/projects/podman-api && go test ./internal/config/... -run TestRenameHostFile -v`
Expected: compile failure, `RenameHostFile`/`HostsDir` undefined.

- [ ] **Step 3: Implement `RenameHostFile` and `HostsDir`**

Add to `internal/config/hosts.go`, after `LoadHosts` (end of file):

```go
// RenameHostFile finds the *.yaml file in dir whose parsed id equals oldID,
// replaces its exact "id: <oldID>" line with "id: <newID>", and atomically
// rewrites the file (temp file + os.Rename, same directory). It does NOT
// re-marshal the YAML — every other line, including comments and formatting,
// is preserved byte-for-byte. Returns the file's path and its ORIGINAL full
// content (before the rewrite), so a caller can restore it verbatim if a
// later step (e.g. a store migration) fails.
//
// Fails closed rather than guessing: if no file's parsed id matches oldID, or
// the id line isn't found in the exact "id: <oldID>" form (e.g. it carries a
// trailing comment), this returns an error and touches nothing.
func RenameHostFile(dir, oldID, newID string) (path string, original []byte, err error) {
	hosts, err := LoadHosts(dir)
	if err != nil {
		return "", nil, fmt.Errorf("rename host file: %w", err)
	}
	found := false
	for _, h := range hosts {
		if h.ID == oldID {
			found = true
			break
		}
	}
	if !found {
		return "", nil, fmt.Errorf("rename host file: no host with id %q in %s", oldID, dir)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", nil, fmt.Errorf("rename host file: read dir %q: %w", dir, err)
	}
	oldLine := "id: " + oldID
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		raw, err := os.ReadFile(p)
		if err != nil {
			return "", nil, fmt.Errorf("rename host file: read %s: %w", p, err)
		}
		lines := strings.Split(string(raw), "\n")
		lineIdx := -1
		for i, l := range lines {
			if l == oldLine {
				lineIdx = i
				break
			}
		}
		if lineIdx == -1 {
			continue // this file isn't the one, or its id line isn't in the expected exact form
		}
		lines[lineIdx] = "id: " + newID
		newContent := strings.Join(lines, "\n")

		tmp := p + ".tmp-rename"
		if err := os.WriteFile(tmp, []byte(newContent), 0o644); err != nil {
			return "", nil, fmt.Errorf("rename host file: write temp file: %w", err)
		}
		if err := os.Rename(tmp, p); err != nil {
			_ = os.Remove(tmp)
			return "", nil, fmt.Errorf("rename host file: replace %s: %w", p, err)
		}
		return p, raw, nil
	}
	return "", nil, fmt.Errorf("rename host file: host %q resolved but no file had the exact line %q", oldID, oldLine)
}

// HostsDir is a directory of hosts/*.yaml files. Its RenameHostFile method
// lets it satisfy internal/api's HostFileRenamer interface without api
// importing config's LoadHosts internals or config importing api.
type HostsDir string

func (d HostsDir) RenameHostFile(oldID, newID string) (path string, original []byte, err error) {
	return RenameHostFile(string(d), oldID, newID)
}
```

Add `"fmt"` to `internal/config/hosts.go`'s imports if not already present (check the existing import block — `os`, `path/filepath`, `strings`, `gopkg.in/yaml.v3` are already there per the file's current contents; `fmt` is used elsewhere in the file already by `LoadHosts`, so it should already be imported — confirm before adding a duplicate).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ~/projects/podman-api && go test ./internal/config/... -run TestRenameHostFile -v && go test ./internal/config/... -run TestHostsDirRenameHostFile -v`
Expected: PASS (all four tests)

- [ ] **Step 5: Run the full config package test suite**

Run: `cd ~/projects/podman-api && go test ./internal/config/... -v 2>&1 | tail -60`
Expected: PASS, no regressions.

- [ ] **Step 6: Commit**

```bash
cd ~/projects/podman-api
git add internal/config/hosts.go internal/config/hosts_test.go
git commit -m "feat(config): add RenameHostFile for comment-preserving host id rewrite"
```

---

### Task 3: `instance.Service.RenameHost`

**Files:**
- Modify: `internal/instance/service.go`
- Test: `internal/instance/service_test.go` (or wherever `TestService...Hosts` style tests already live — grep `func TestService` in `internal/instance/*_test.go` and place alongside similar host-scoped tests)

**Interfaces:**
- Consumes: `store.RenameHost(ctx, oldID, newID string) error` (Task 1), `store.ErrHostRenameConflict`, `store.ErrHostRenameHasBackups`, `Service.Hosts() []config.Host` (existing), `Service.SetHosts(hosts []config.Host)` (existing).
- Produces: `Service.RenameHost(ctx context.Context, oldID, newID string) error`, and two new sentinel errors `instance.ErrHostAlreadyExists`, `instance.ErrHostHasBackups`, alongside the existing `instance.ErrUnknownHost` (`internal/instance/service.go:25`).

- [ ] **Step 1: Write the failing test**

First check `internal/instance/*_test.go` for how other `Service` tests build a `*Service` with a `store.Memory` and a fake podman client (grep `instance.NewService(fake.New()` or similar in the package's own test files, not `internal/api`'s), and match that construction exactly. Then add:

```go
func TestServiceRenameHost(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := NewService(fake.New(), hosts)
	st := store.NewMemory()
	svc.SetStore(st)
	ctx := context.Background()

	require.NoError(t, st.PutSpec(ctx, store.Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}}))

	require.NoError(t, svc.RenameHost(ctx, "h1", "h2"))

	got := svc.Hosts()
	require.Len(t, got, 1)
	require.Equal(t, "h2", got[0].ID)

	_, err := st.GetSpec(ctx, "h2", "app", "a")
	require.NoError(t, err)
}

func TestServiceRenameHostUnknown(t *testing.T) {
	svc := NewService(fake.New(), nil)
	svc.SetStore(store.NewMemory())

	err := svc.RenameHost(context.Background(), "nope", "h2")
	require.ErrorIs(t, err, ErrUnknownHost)
}

func TestServiceRenameHostConflict(t *testing.T) {
	hosts := []config.Host{{ID: "h1"}, {ID: "h2"}}
	svc := NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())

	err := svc.RenameHost(context.Background(), "h1", "h2")
	require.ErrorIs(t, err, ErrHostAlreadyExists)
}

func TestServiceRenameHostHasBackups(t *testing.T) {
	hosts := []config.Host{{ID: "h1"}}
	svc := NewService(fake.New(), hosts)
	st := store.NewMemory()
	svc.SetStore(st)
	ctx := context.Background()
	require.NoError(t, st.PutSpec(ctx, store.Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}}))
	require.NoError(t, st.CreateBackup(ctx, store.Backup{ID: "b1", Host: "h1", Template: "app", Slug: "a", State: store.BackupStateCreating, Created: time.Now()}))

	err := svc.RenameHost(ctx, "h1", "h2")
	require.ErrorIs(t, err, ErrHostHasBackups)
}
```

Use the confirmed real `BackupState` constant name from Task 1 Step 3 if it differs from `store.BackupStateCreating`.

- [ ] **Step 2: Run tests to verify they fail to compile**

Run: `cd ~/projects/podman-api && go test ./internal/instance/... -run TestServiceRenameHost -v`
Expected: compile failure — `RenameHost`, `ErrHostAlreadyExists`, `ErrHostHasBackups` undefined on `Service`/package `instance`.

- [ ] **Step 3: Add the two sentinel errors**

In `internal/instance/service.go`, find the existing `ErrUnknownHost = errors.New("unknown host")` (line 25) and add immediately after it in the same var block:

```go
	ErrHostAlreadyExists = errors.New("host already exists")
	ErrHostHasBackups    = errors.New("host has backups; rename would orphan their blob storage")
```

- [ ] **Step 4: Implement `Service.RenameHost`**

Add to `internal/instance/service.go`, near the other `Hosts`/`SetHosts` methods (after `Hosts()` at line 1373):

```go
// RenameHost migrates a host's store rows (specs, host secrets, backups) from
// oldID to newID and, on success, updates the live host set so the new id is
// resolvable immediately — no restart or SIGHUP required. Returns
// ErrUnknownHost if oldID isn't a currently-configured host,
// ErrHostAlreadyExists if newID collides with a store.ErrHostRenameConflict
// (typically stale rows under an id no host currently claims — the common
// "newID is a live host" case is expected to be checked by the caller before
// this is reached, e.g. the API handler, but this is the defensive backstop),
// and ErrHostHasBackups if oldID has any backups (S3 blob re-keying is out of
// scope; see docs/superpowers/specs/2026-08-10-host-rename-design.md).
func (s *Service) RenameHost(ctx context.Context, oldID, newID string) error {
	hosts := s.Hosts()
	found := false
	for _, h := range hosts {
		if h.ID == oldID {
			found = true
			break
		}
	}
	if !found {
		return ErrUnknownHost
	}

	if err := s.store.RenameHost(ctx, oldID, newID); err != nil {
		switch {
		case errors.Is(err, store.ErrHostRenameConflict):
			return ErrHostAlreadyExists
		case errors.Is(err, store.ErrHostRenameHasBackups):
			return ErrHostHasBackups
		default:
			return err
		}
	}

	for i := range hosts {
		if hosts[i].ID == oldID {
			hosts[i].ID = newID
		}
	}
	s.SetHosts(hosts)
	return nil
}
```

`errors` and `store` are already imported by `internal/instance/service.go` (confirm before assuming — grep the top import block; both are used extensively elsewhere in this file).

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd ~/projects/podman-api && go test ./internal/instance/... -run TestServiceRenameHost -v`
Expected: PASS (all four tests)

- [ ] **Step 6: Run the full instance package test suite**

Run: `cd ~/projects/podman-api && go test ./internal/instance/... 2>&1 | tail -80`
Expected: PASS, no regressions. (This package's suite is large — if anything unrelated is already failing on this branch before your change, note it and confirm it's pre-existing via `git stash` + re-run, don't attribute it to this task.)

- [ ] **Step 7: Commit**

```bash
cd ~/projects/podman-api
git add internal/instance/service.go internal/instance/service_test.go
git commit -m "feat(instance): add Service.RenameHost orchestrating store + live host swap"
```

(Adjust the test file path in the `git add` to whatever file you actually added the tests to in Step 1.)

---

### Task 4: API route `POST /hosts/{host}/rename`

**Files:**
- Modify: `internal/api/router.go` (scope, route, `HostFileRenamer` interface, `handlers` field, `WithHostRenamer` option)
- Modify: `internal/api/hosts.go` (handler)
- Modify: `internal/api/errors.go` (classify cases for the two new sentinel errors)
- Test: `internal/api/hosts_test.go`

**Interfaces:**
- Consumes: `Service.RenameHost` (Task 3), `Service.Hosts()` (existing), `instance.ErrHostAlreadyExists`/`ErrHostHasBackups`/`ErrUnknownHost` (Task 3 + existing), `config.HostsDir` (Task 2, used only in the test and later in Task 5's server wiring — not imported by `internal/api` itself, to avoid a dependency the package doesn't need beyond the interface shape).
- Produces: route `POST /hosts/{host}/rename` guarded by scope `hosts:write`; `api.HostFileRenamer` interface; `api.WithHostRenamer(r HostFileRenamer) RouterOption`.

- [ ] **Step 1: Add the `HostFileRenamer` interface, `handlers` field, and `WithHostRenamer` option**

In `internal/api/router.go`, add the interface near the top (after the imports, before `NewRouter`):

```go
// HostFileRenamer rewrites a host's id in its hosts/*.yaml config file,
// returning the file's path and its original content (for revert-on-failure).
// Implemented by config.HostsDir.
type HostFileRenamer interface {
	RenameHostFile(oldID, newID string) (path string, original []byte, err error)
}
```

Add a field to the `handlers` struct (currently ending `pruner RegistryPruner` at line 154):

```go
type handlers struct {
	svc         *instance.Service
	jobs        store.JobStore
	canceller   JobCanceller
	registry    imgregistry.Client
	pruner      RegistryPruner
	hostRenamer HostFileRenamer
}
```

Add the option next to `WithRegistryPruner` (after line 167):

```go
// WithHostRenamer enables POST /hosts/{host}/rename. Without it the route
// responds 501, the same "absent, not disabled" shape as the registry browse
// routes without a client.
func WithHostRenamer(r HostFileRenamer) RouterOption {
	return func(h *handlers) { h.hostRenamer = r }
}
```

Add the route, in the "Hosts (read)" section's sibling — add a new comment block right after the existing four `GET /hosts...` lines (around line 55):

```go
	// Host rename. 501 when no HostFileRenamer is wired (WithHostRenamer).
	mux.Handle("POST /hosts/{host}/rename", guard("hosts:write", http.HandlerFunc(h.renameHost)))
```

- [ ] **Step 2: Add classify() cases for the two new sentinel errors**

In `internal/api/errors.go`, add two cases to the `classify` function, near the existing `instance.ErrInstanceExists`/`instance.ErrTemplateExists` conflict cases:

```go
	case errors.Is(err, instance.ErrHostAlreadyExists):
		return "host_already_exists", http.StatusConflict, err.Error()
	case errors.Is(err, instance.ErrHostHasBackups):
		return "host_has_backups", http.StatusConflict, err.Error()
```

- [ ] **Step 3: Write the failing handler tests**

Add to `internal/api/hosts_test.go`, following the exact `TestListHosts` construction pattern already in that file (server via `NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil, api.WithHostRenamer(...))` — note this file is `package api` per its existing header, so no `api.` prefix is needed for same-package identifiers; only add it if the test file turns out to be `package api_test`, which the Step-1-read of the file will show):

```go
type fakeHostRenamer struct {
	calledOld, calledNew string
	original             []byte
	err                  error
}

func (f *fakeHostRenamer) RenameHostFile(oldID, newID string) (string, []byte, error) {
	f.calledOld, f.calledNew = oldID, newID
	if f.err != nil {
		return "", nil, f.err
	}
	return "/fake/path.yaml", f.original, nil
}

func TestRenameHost(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write", "hosts:read"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())
	renamer := &fakeHostRenamer{original: []byte("id: h1\n")}
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil, WithHostRenamer(renamer)))
	defer srv.Close()

	req, err := http.NewRequest("POST", srv.URL+"/hosts/h1/rename", strings.NewReader(`{"new_id":"h2"}`))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "h1", renamer.calledOld)
	require.Equal(t, "h2", renamer.calledNew)

	// Old id gone, new id live — no restart between the calls.
	resp2 := authedReq(t, srv, tok, "GET", "/hosts/h1")
	defer resp2.Body.Close()
	require.Equal(t, http.StatusNotFound, resp2.StatusCode)

	resp3 := authedReq(t, srv, tok, "GET", "/hosts/h2")
	defer resp3.Body.Close()
	require.Equal(t, http.StatusOK, resp3.StatusCode)
}

func TestRenameHostUnknownHost(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write"}}}
	svc := instance.NewService(fake.New(), nil)
	svc.SetStore(store.NewMemory())
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil, WithHostRenamer(&fakeHostRenamer{})))
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/hosts/nope/rename", strings.NewReader(`{"new_id":"h2"}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestRenameHostConflict(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write"}}}
	hosts := []config.Host{{ID: "h1"}, {ID: "h2"}}
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil, WithHostRenamer(&fakeHostRenamer{})))
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/rename", strings.NewReader(`{"new_id":"h2"}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestRenameHostFileRenameFailsLeavesStoreUntouched(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write"}}}
	hosts := []config.Host{{ID: "h1"}}
	svc := instance.NewService(fake.New(), hosts)
	st := store.NewMemory()
	svc.SetStore(st)
	renamer := &fakeHostRenamer{err: errors.New("disk full")}
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil, WithHostRenamer(renamer)))
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/rename", strings.NewReader(`{"new_id":"h2"}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	got := svc.Hosts()
	require.Len(t, got, 1)
	require.Equal(t, "h1", got[0].ID) // unchanged
}

func TestRenameHostNoRenamerConfigured(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write"}}}
	hosts := []config.Host{{ID: "h1"}}
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil)) // no WithHostRenamer
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/rename", strings.NewReader(`{"new_id":"h2"}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}
```

Add `"errors"` and `"strings"` to the test file's imports if not already present.

- [ ] **Step 4: Run tests to verify they fail**

Run: `cd ~/projects/podman-api && go test ./internal/api/... -run TestRenameHost -v`
Expected: compile failure — `renameHost` handler, `WithHostRenamer`, route not wired yet.

- [ ] **Step 5: Implement the handler**

Add to `internal/api/hosts.go`:

```go
func (h *handlers) renameHost(w http.ResponseWriter, r *http.Request) {
	oldID := r.PathValue("host")
	var req struct {
		NewID string `json:"new_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if req.NewID == "" {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "new_id is required"})
		return
	}
	if !validName(req.NewID) {
		writeInvalidName(w, "new_id", req.NewID)
		return
	}
	if req.NewID == oldID {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "new_id must differ from the current host id"})
		return
	}

	found := false
	for _, hh := range h.svc.Hosts() {
		if hh.ID == oldID {
			found = true
		}
		if hh.ID == req.NewID {
			WriteJSON(w, http.StatusConflict, ErrorBody{Code: "host_already_exists", Message: fmt.Sprintf("host %q already exists", req.NewID)})
			return
		}
	}
	if !found {
		WriteError(w, instance.ErrUnknownHost)
		return
	}

	if h.hostRenamer == nil {
		WriteJSON(w, http.StatusNotImplemented, ErrorBody{Code: "not_implemented", Message: "host rename is not configured on this server"})
		return
	}

	path, original, err := h.hostRenamer.RenameHostFile(oldID, req.NewID)
	if err != nil {
		WriteJSON(w, http.StatusInternalServerError, ErrorBody{Code: "internal", Message: err.Error()})
		return
	}

	if err := h.svc.RenameHost(r.Context(), oldID, req.NewID); err != nil {
		if werr := os.WriteFile(path, original, 0o644); werr != nil {
			log.Printf("host rename %s -> %s: store migration failed (%v) AND reverting %s failed (%v) — config file now inconsistent with the store, fix by hand", oldID, req.NewID, err, path, werr)
		}
		WriteError(w, err)
		return
	}

	WriteJSON(w, http.StatusOK, map[string]string{"old_id": oldID, "new_id": req.NewID})
}
```

Add `"fmt"`, `"log"`, and `"os"` to `internal/api/hosts.go`'s imports (check the existing import block first — `context`, `net/http`, `sync`, `time`, plus the three internal packages are already there per the file's current contents; `encoding/json` is used by `renameHost` too, confirm it's present or add it).

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd ~/projects/podman-api && go test ./internal/api/... -run TestRenameHost -v`
Expected: PASS (all five tests)

- [ ] **Step 7: Run the full api package test suite**

Run: `cd ~/projects/podman-api && go test ./internal/api/... 2>&1 | tail -80`
Expected: PASS, no regressions.

- [ ] **Step 8: Commit**

```bash
cd ~/projects/podman-api
git add internal/api/router.go internal/api/hosts.go internal/api/errors.go internal/api/hosts_test.go
git commit -m "feat(api): add POST /hosts/{host}/rename"
```

---

### Task 5: Wire into `server/server.go`, add `hosts:write` to discovery docs, update README/CLAUDE.md

**Files:**
- Modify: `server/server.go`
- Modify: `internal/api/discovery.go` (if it enumerates scopes/routes for `/mcp` or `/agent-docs` — check first)
- Modify: `api/openapi.yaml` (if this repo maintains a real OpenAPI spec file consumed by `GET /openapi.yaml` — check `apispec.Spec` in router.go's import, `github.com/iotready/podman-api/api`)
- Modify: `README.md` (scope list, if one is documented there)
- Test: none new (this task is wiring + docs; covered by Task 4's tests plus a manual smoke check)

**Interfaces:**
- Consumes: `api.WithHostRenamer` (Task 4), `config.HostsDir` (Task 2).

- [ ] **Step 1: Wire the router option in `server/server.go`**

At line 531 (the existing `router := api.NewRouter(...)` call), change:

```go
	router := api.NewRouter(svc, jobStore, keyStore, combined, nil, canceller, Version, registryClient, routerOpts...)
```

to add the host renamer to `routerOpts` before this line — insert right after the existing `if regPruneSched != nil { ... }` block (around line 528):

```go
	routerOpts = append(routerOpts, api.WithHostRenamer(config.HostsDir(*hostsDir)))
```

`config` is already imported by `server/server.go` (it calls `config.LoadHosts(*hostsDir)` at line 137).

- [ ] **Step 2: Check whether `internal/api/discovery.go` or `api/openapi.yaml` enumerate routes/scopes that need the new route added**

Run: `cd ~/projects/podman-api && grep -n "instances:write\|POST /hosts" internal/api/discovery.go api/openapi.yaml 2>/dev/null`

If either file lists routes/scopes explicitly (rather than generating them from the router), add the new route/scope following the exact existing entries' format for `POST /hosts/{host}/instances/{template}/{slug}/rename` as a template. If neither file enumerates routes this way (e.g. `openapi.yaml` might be generated, or discovery.go might just link to it), skip this step and note in the commit message that no doc file needed a matching entry.

- [ ] **Step 3: Check whether `README.md` documents the scope list**

Run: `cd ~/projects/podman-api && grep -n "instances:write\|hosts:read" README.md`

If found, add `hosts:write` to the same list/table, following the existing format.

- [ ] **Step 4: Build and run the full test suite**

Run: `cd ~/projects/podman-api && make build && make test`
Expected: clean build, all tests pass.

- [ ] **Step 5: Run `make vet`**

Run: `cd ~/projects/podman-api && make vet`
Expected: clean (gofmt + go vet).

- [ ] **Step 6: Commit**

```bash
cd ~/projects/podman-api
git add server/server.go
# plus internal/api/discovery.go, api/openapi.yaml, README.md if Steps 2-3 touched them
git commit -m "feat: wire host rename into server, document hosts:write scope"
```

---

### Task 6: Live validation on `engine-2`

Not a code task — this is the live test the user asked for, run from `podman-api-pro`'s environment against the deployed fleet. Requires Tasks 1-5 built, tagged, and deployed to `engine-infra` first (see below).

**Pre-requisites:**
- All of Tasks 1-5 committed on a feature branch in `~/projects/podman-api`.
- Open a PR (`forgejo pr create`), get it merged to `main`, tag a release per this repo's normal release flow (check `CLAUDE.md`/`Makefile` in `~/projects/podman-api` for the tagging command — likely `make release` or a tag+push, confirm before tagging).
- In `~/projects/podman-api-pro`: `make bump V=<new-tag>`, `make build && make test`, commit `go.mod`/`go.sum`.
- Deploy the bumped `podman-api-pro` binary to `engine-infra` (the control plane) per `CLAUDE.md`'s existing deploy recipe: `make build-linux && scp bin/podman-api-pro engine-infra:~/.local/bin/podman-api && ssh engine-infra "systemctl --user restart podman-api"`.
- **Before deploying to `engine-infra`: confirm with the user first** — this restarts the live control plane serving the whole fleet, which is a shared-system action per this session's own risk-review norms, not something to do silently as part of "continue."

- [ ] **Step 1: Confirm engine-2 still has no backup rows** (the v1 refusal condition)

```sh
set -a; . ~/.config/podman-api/pro.env; set +a
curl -sS -H "Authorization: Bearer $PODMAN_API_TOKEN" "$PODMAN_API_ADDR/hosts/engine-2/instances" | python3 -c "
import json,sys
for i in json.load(sys.stdin):
    print(i['template'], i['slug'])
"
# For each, check GET .../backups is empty:
# curl -sS -H \"Authorization: Bearer \$PODMAN_API_TOKEN\" \"\$PODMAN_API_ADDR/hosts/engine-2/instances/<template>/<slug>/backups\"
```

If any instance has backup rows, this test needs a host without backups instead — stop and pick a different target with the user rather than hitting the v1 refusal mid-test.

- [ ] **Step 2: Rename `engine-2` -> `engine-2-rntest`**

```sh
curl -sS -X POST -H "Authorization: Bearer $PODMAN_API_TOKEN" -H 'Content-Type: application/json' \
  "$PODMAN_API_ADDR/hosts/engine-2/rename" -d '{"new_id":"engine-2-rntest"}'
```

Expected: `200 {"old_id":"engine-2","new_id":"engine-2-rntest"}`.

- [ ] **Step 3: Verify the new id is live immediately (no SIGHUP/restart)**

```sh
curl -sS -H "Authorization: Bearer $PODMAN_API_TOKEN" "$PODMAN_API_ADDR/hosts/engine-2-rntest/instances" | python3 -m json.tool | head -40
curl -sS -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer $PODMAN_API_TOKEN" "$PODMAN_API_ADDR/hosts/engine-2/instances"
```

Expected: first call returns all 7 instances (`alloy/main`, `caddy/edge`, `forgejo-runner/e2a`, `forgejo-runner/e2b`, `mariadb/frappe`, `node-exporter/main`, `redis/frappe`), all still `ready`; second call is `404`.

- [ ] **Step 4: Verify the running pods themselves were never touched**

```sh
ssh engine-2 "podman pod ps"
```

Expected: same pod IDs/creation timestamps as before the rename (compare against the `created` timestamps captured earlier in this conversation — `caddy/edge` created `2026-08-09T18:19:07Z`, `alloy/main` created `2026-07-25T08:07:33Z`, etc.) — proving the rename never restarted anything.

- [ ] **Step 5: Verify the host config file was actually rewritten**

```sh
ssh engine-infra "grep -A2 '^id: engine-2-rntest' <hosts-dir-path>/*.yaml"
```

(Substitute the real hosts dir path on `engine-infra` — check the deployed systemd unit's `-hosts-dir` flag value, or default `hosts/` relative to the binary's working directory, via `ssh engine-infra "systemctl --user cat podman-api"` or equivalent.)

- [ ] **Step 6: Rename back to `engine-2`**

```sh
curl -sS -X POST -H "Authorization: Bearer $PODMAN_API_TOKEN" -H 'Content-Type: application/json' \
  "$PODMAN_API_ADDR/hosts/engine-2-rntest/rename" -d '{"new_id":"engine-2"}'
```

Expected: `200`.

- [ ] **Step 7: Re-verify end state matches the pre-test state**

```sh
curl -sS -H "Authorization: Bearer $PODMAN_API_TOKEN" "$PODMAN_API_ADDR/hosts/engine-2/instances" | python3 -c "
import json,sys
for i in json.load(sys.stdin):
    print(i['template'], i['slug'], i.get('ready'))
"
```

Expected: identical to the listing captured before Step 2 — same 7 instances, all `ready: true`.

- [ ] **Step 8: Report results to the user** — do not consider this task done until you've shown the before/after listings and the pod-timestamp comparison from Step 4 confirming nothing on `engine-2` was disturbed.
