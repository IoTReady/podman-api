# State Store Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in, SQLite-backed store that persists each instance's desired state (`parameters` + AES-256-GCM-encrypted `secrets`) on Apply and removes it on Delete, so later phases can migrate/evacuate by name.

**Architecture:** A new pure-Go `internal/store` package exposes a small `Store` interface (`PutSpec`/`GetSpec`/`DeleteSpec`) with a SQLite implementation. Secrets are sealed with AES-256-GCM using a 32-byte key held in a hot-reloadable `KeyStore` (mirrors `internal/auth.KeyStore`). `instance.Service` gains a nil-able `store` field wired via a `SetStore` setter (mirrors `SetHosts`); when nil the daemon behaves exactly as today. Two flags (`-state-db`, `-spec-key-file`) enable it in `main`.

**Tech Stack:** Go, `modernc.org/sqlite` (pure-Go driver, new direct dep), `crypto/aes` + `crypto/cipher` (GCM), `database/sql`.

**Spec:** `docs/superpowers/specs/2026-06-03-state-store-foundation-design.md`

---

## Conventions for every task

- **Build tags.** The full binary needs the podman remote-client tags. Define once per shell:
  ```sh
  export TAGS="containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper"
  ```
  - Tasks 1–4 touch only `internal/store` (pure Go, no podman import) — tests run **with or without** tags. Plain `go test ./internal/store/` works.
  - Tasks 5–6 touch packages that import podman — tests **must** use `-tags "$TAGS"`.
- **gofmt + vet** must stay clean: `gofmt -l .` empty, `go vet -tags "$TAGS" ./...` clean.
- **Commit trailer** on every commit:
  ```
  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  ```
- This is a Forgejo repo (`tej/podman-api`); do not use `gh`.

## File structure

| File | Responsibility | Task |
| --- | --- | --- |
| `internal/store/crypto.go` | `seal`/`open` — AES-256-GCM, nonce-prefixed | 1 |
| `internal/store/crypto_test.go` | crypto round-trip / tamper tests | 1 |
| `internal/store/key.go` | `KeyStore` + `LoadKeyFile` | 2 |
| `internal/store/key_test.go` | key load + atomic swap tests | 2 |
| `internal/store/store.go` | `Store` interface, `Spec`, `ErrNotFound` | 3 |
| `internal/store/memory.go` | in-memory `Store` + error hooks (test double) | 3 |
| `internal/store/memory_test.go` | memory round-trip tests | 3 |
| `internal/store/sqlite.go` | `OpenSQLite`, schema, Put/Get/Delete | 4 |
| `internal/store/sqlite_test.go` | sqlite round-trip / upsert / wrong-key tests | 4 |
| `internal/instance/service.go` | `store` field, `SetStore`, persist on Apply, remove on Delete | 5 |
| `internal/instance/service_test.go` | Service+store integration tests | 5 |
| `cmd/podman-api/main.go` | `-state-db`/`-spec-key-file` flags, `openStore`, SIGHUP key reload | 6 |
| `cmd/podman-api/main_test.go` | `openStore` unit tests | 6 |
| `README.md` | document the two flags + key generation | 7 |

---

## Task 1: Secret encryption (`seal` / `open`)

**Files:**
- Create: `internal/store/crypto.go`
- Test: `internal/store/crypto_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package store

import (
	"bytes"
	"testing"
)

func testKey(b byte) [32]byte {
	var k [32]byte
	for i := range k {
		k[i] = b
	}
	return k
}

func TestSealOpen_RoundTrip(t *testing.T) {
	key := testKey(0x11)
	plain := []byte(`{"password":"hunter2"}`)
	blob, err := seal(key, plain)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if bytes.Equal(blob, plain) {
		t.Fatal("ciphertext equals plaintext")
	}
	got, err := open(key, blob)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, plain)
	}
}

func TestOpen_WrongKey_Fails(t *testing.T) {
	blob, err := seal(testKey(0x11), []byte("secret"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if _, err := open(testKey(0x22), blob); err == nil {
		t.Fatal("open with wrong key should fail")
	}
}

func TestSeal_NonceUniqueness(t *testing.T) {
	key := testKey(0x11)
	a, _ := seal(key, []byte("x"))
	b, _ := seal(key, []byte("x"))
	if bytes.Equal(a, b) {
		t.Fatal("two seals of same plaintext are identical (nonce not random)")
	}
}

func TestOpen_TooShort_Fails(t *testing.T) {
	if _, err := open(testKey(0x11), []byte{0x00, 0x01}); err == nil {
		t.Fatal("open of too-short blob should fail")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/store/ -run 'TestSealOpen|TestOpen|TestSeal' -v`
Expected: FAIL — `undefined: seal` / `undefined: open`.

- [ ] **Step 3: Write the implementation**

```go
package store

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

// seal encrypts plaintext with AES-256-GCM under key, returning
// nonce || ciphertext. A fresh random nonce is generated per call.
func seal(key [32]byte, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	// Seal appends the ciphertext to nonce, so the result is nonce||ct.
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// open reverses seal. blob must be nonce || ciphertext as produced by seal.
func open(key [32]byte, blob []byte) ([]byte, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	ns := gcm.NonceSize()
	if len(blob) < ns {
		return nil, errors.New("store: ciphertext too short")
	}
	nonce, ct := blob[:ns], blob[ns:]
	return gcm.Open(nil, nonce, ct, nil)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/store/ -run 'TestSealOpen|TestOpen|TestSeal' -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/store/crypto.go internal/store/crypto_test.go
git add internal/store/crypto.go internal/store/crypto_test.go
git commit -m "feat(store): AES-256-GCM seal/open for secret blobs (#31)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Encryption key — `KeyStore` and `LoadKeyFile`

**Files:**
- Create: `internal/store/key.go`
- Test: `internal/store/key_test.go`

Mirrors `internal/auth/store.go` (atomic, hot-swappable). The key is 32 raw bytes, accepted either as a raw 32-byte file or base64-of-32-bytes.

- [ ] **Step 1: Write the failing tests**

```go
package store

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

func TestLoadKeyFile_Raw32(t *testing.T) {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(i)
	}
	k, err := LoadKeyFile(writeFile(t, "raw.key", raw))
	if err != nil {
		t.Fatalf("LoadKeyFile: %v", err)
	}
	if k != *(*[32]byte)(raw) {
		t.Fatal("raw key mismatch")
	}
}

func TestLoadKeyFile_Base64WithNewline(t *testing.T) {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(0xA0 + i)
	}
	enc := base64.StdEncoding.EncodeToString(raw) + "\n"
	k, err := LoadKeyFile(writeFile(t, "b64.key", []byte(enc)))
	if err != nil {
		t.Fatalf("LoadKeyFile: %v", err)
	}
	if k != *(*[32]byte)(raw) {
		t.Fatal("base64 key mismatch")
	}
}

func TestLoadKeyFile_WrongLength(t *testing.T) {
	if _, err := LoadKeyFile(writeFile(t, "short.key", []byte("too short"))); err == nil {
		t.Fatal("expected error for non-32-byte key")
	}
}

func TestLoadKeyFile_Missing(t *testing.T) {
	if _, err := LoadKeyFile(filepath.Join(t.TempDir(), "nope.key")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestKeyStore_StoreLoad(t *testing.T) {
	ks := NewKeyStore(testKey(0x01))
	if ks.Load() != testKey(0x01) {
		t.Fatal("initial key mismatch")
	}
	ks.Store(testKey(0x02))
	if ks.Load() != testKey(0x02) {
		t.Fatal("after Store, key not updated")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/store/ -run 'TestLoadKeyFile|TestKeyStore' -v`
Expected: FAIL — `undefined: LoadKeyFile` / `undefined: NewKeyStore`.

- [ ] **Step 3: Write the implementation**

```go
package store

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"sync/atomic"
)

// KeyStore is an atomically-swappable holder for the 32-byte secret key.
// Safe for concurrent Load/Store; mirrors internal/auth.KeyStore so a SIGHUP
// reload in main takes effect on the next seal/open without a restart.
type KeyStore struct {
	key atomic.Pointer[[32]byte]
}

// NewKeyStore returns a store seeded with k.
func NewKeyStore(k [32]byte) *KeyStore {
	s := &KeyStore{}
	s.Store(k)
	return s
}

// Store atomically replaces the live key.
func (s *KeyStore) Store(k [32]byte) {
	kk := k
	s.key.Store(&kk)
}

// Load returns the current key (zero value if never set).
func (s *KeyStore) Load() [32]byte {
	p := s.key.Load()
	if p == nil {
		return [32]byte{}
	}
	return *p
}

// LoadKeyFile reads a 32-byte encryption key from path. The file may contain
// either the 32 raw bytes, or the base64 (std) encoding of 32 bytes. Trailing
// whitespace/newlines are ignored. Anything else is an error.
func LoadKeyFile(path string) ([32]byte, error) {
	var k [32]byte
	raw, err := os.ReadFile(path)
	if err != nil {
		return k, err
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 32 {
		copy(k[:], trimmed)
		return k, nil
	}
	if dec, err := base64.StdEncoding.DecodeString(string(trimmed)); err == nil && len(dec) == 32 {
		copy(k[:], dec)
		return k, nil
	}
	return k, fmt.Errorf("store: spec key in %s must be 32 raw bytes or base64 of 32 bytes", path)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/store/ -run 'TestLoadKeyFile|TestKeyStore' -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/store/key.go internal/store/key_test.go
git add internal/store/key.go internal/store/key_test.go
git commit -m "feat(store): hot-reloadable encryption KeyStore + LoadKeyFile (#31)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `Store` interface, `Spec`, and in-memory test double

**Files:**
- Create: `internal/store/store.go`
- Create: `internal/store/memory.go`
- Test: `internal/store/memory_test.go`

The `Memory` store backs the Service-layer unit tests in Task 5 (no SQLite/key needed there). It exposes `PutErr`/`DeleteErr` hooks to drive the fatal-failure paths.

- [ ] **Step 1: Write the failing tests**

```go
package store

import (
	"context"
	"errors"
	"testing"
)

func sampleSpec() Spec {
	return Spec{
		Host: "h1", Template: "postgres", Slug: "demo",
		Parameters: map[string]any{"image": "postgres:16", "user": "app"},
		Secrets:    map[string]string{"password": "hunter2"},
	}
}

func TestMemory_PutGetDelete(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	if err := m.PutSpec(ctx, sampleSpec()); err != nil {
		t.Fatalf("PutSpec: %v", err)
	}
	got, err := m.GetSpec(ctx, "h1", "postgres", "demo")
	if err != nil {
		t.Fatalf("GetSpec: %v", err)
	}
	if got.Secrets["password"] != "hunter2" || got.Parameters["image"] != "postgres:16" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	if err := m.DeleteSpec(ctx, "h1", "postgres", "demo"); err != nil {
		t.Fatalf("DeleteSpec: %v", err)
	}
	if _, err := m.GetSpec(ctx, "h1", "postgres", "demo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_GetMissing(t *testing.T) {
	if _, err := NewMemory().GetSpec(context.Background(), "h1", "x", "y"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_DeleteMissing(t *testing.T) {
	if err := NewMemory().DeleteSpec(context.Background(), "h1", "x", "y"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_ErrorHooks(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	m.PutErr = errors.New("put boom")
	if err := m.PutSpec(ctx, sampleSpec()); err == nil {
		t.Fatal("expected PutErr")
	}
	m.PutErr = nil
	_ = m.PutSpec(ctx, sampleSpec())
	m.DeleteErr = errors.New("del boom")
	if err := m.DeleteSpec(ctx, "h1", "postgres", "demo"); err == nil {
		t.Fatal("expected DeleteErr")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/store/ -run TestMemory -v`
Expected: FAIL — `undefined: NewMemory` / `undefined: ErrNotFound`.

- [ ] **Step 3: Write the implementations**

`internal/store/store.go`:

```go
// Package store is the daemon's durable desired-state record: one encrypted
// row per instance, written on Apply and removed on Delete. It is opt-in; when
// no store is wired the daemon is a stateless proxy as before.
package store

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by GetSpec/DeleteSpec when no row matches.
var ErrNotFound = errors.New("store: spec not found")

// Spec is the desired state of one instance.
type Spec struct {
	Host     string
	Template string
	Slug     string
	Parameters map[string]any
	Secrets    map[string]string
	Created    time.Time
	Updated    time.Time
}

// Store persists instance specs. Implementations encrypt Secrets at rest.
type Store interface {
	PutSpec(ctx context.Context, s Spec) error
	GetSpec(ctx context.Context, host, template, slug string) (Spec, error)
	DeleteSpec(ctx context.Context, host, template, slug string) error
}
```

`internal/store/memory.go`:

```go
package store

import (
	"context"
	"sync"
)

// Memory is an in-memory Store for tests. PutErr/DeleteErr, when non-nil, make
// the corresponding call fail — used to exercise the fatal-failure paths.
type Memory struct {
	mu    sync.Mutex
	specs map[string]Spec

	PutErr    error
	DeleteErr error
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{specs: map[string]Spec{}}
}

func memKey(host, template, slug string) string {
	return host + "|" + template + "|" + slug
}

func (m *Memory) PutSpec(_ context.Context, s Spec) error {
	if m.PutErr != nil {
		return m.PutErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.specs[memKey(s.Host, s.Template, s.Slug)] = s
	return nil
}

func (m *Memory) GetSpec(_ context.Context, host, template, slug string) (Spec, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.specs[memKey(host, template, slug)]
	if !ok {
		return Spec{}, ErrNotFound
	}
	return s, nil
}

func (m *Memory) DeleteSpec(_ context.Context, host, template, slug string) error {
	if m.DeleteErr != nil {
		return m.DeleteErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k := memKey(host, template, slug)
	if _, ok := m.specs[k]; !ok {
		return ErrNotFound
	}
	delete(m.specs, k)
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/store/ -run TestMemory -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/store/store.go internal/store/memory.go internal/store/memory_test.go
git add internal/store/store.go internal/store/memory.go internal/store/memory_test.go
git commit -m "feat(store): Store interface, Spec, in-memory test double (#31)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: SQLite implementation

**Files:**
- Create: `internal/store/sqlite.go`
- Test: `internal/store/sqlite_test.go`
- Modify: `go.mod` / `go.sum` (add `modernc.org/sqlite`)

- [ ] **Step 1: Add the driver dependency**

Run:
```bash
go get modernc.org/sqlite@latest
go mod tidy -tags "$TAGS"
```
Expected: `modernc.org/sqlite` appears as a **direct** require in `go.mod`; `go.sum` updated. (`-tags` keeps `go mod tidy` from dropping the podman-only deps.)

- [ ] **Step 2: Write the failing tests**

```go
package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func openTestStore(t *testing.T, ks *KeyStore) *SQLite {
	t.Helper()
	db := filepath.Join(t.TempDir(), "state.db")
	s, err := OpenSQLite(db, ks)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSQLite_PutGet_RoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	if err := s.PutSpec(ctx, sampleSpec()); err != nil {
		t.Fatalf("PutSpec: %v", err)
	}
	got, err := s.GetSpec(ctx, "h1", "postgres", "demo")
	if err != nil {
		t.Fatalf("GetSpec: %v", err)
	}
	if got.Secrets["password"] != "hunter2" {
		t.Fatalf("secret not decrypted: %+v", got.Secrets)
	}
	if got.Parameters["user"] != "app" {
		t.Fatalf("parameter mismatch: %+v", got.Parameters)
	}
	if got.Created.IsZero() || got.Updated.IsZero() {
		t.Fatal("timestamps not set")
	}
}

func TestSQLite_GetMissing(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	if _, err := s.GetSpec(context.Background(), "h1", "x", "y"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestSQLite_Delete(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	_ = s.PutSpec(ctx, sampleSpec())
	if err := s.DeleteSpec(ctx, "h1", "postgres", "demo"); err != nil {
		t.Fatalf("DeleteSpec: %v", err)
	}
	if _, err := s.GetSpec(ctx, "h1", "postgres", "demo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
	if err := s.DeleteSpec(ctx, "h1", "postgres", "demo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete of absent row should return ErrNotFound, got %v", err)
	}
}

func TestSQLite_Upsert_PreservesCreated(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	_ = s.PutSpec(ctx, sampleSpec())
	first, _ := s.GetSpec(ctx, "h1", "postgres", "demo")

	// Re-put with changed secret/param (rotation).
	sp := sampleSpec()
	sp.Secrets["password"] = "rotated"
	sp.Parameters["user"] = "admin"
	if err := s.PutSpec(ctx, sp); err != nil {
		t.Fatalf("re-PutSpec: %v", err)
	}
	second, _ := s.GetSpec(ctx, "h1", "postgres", "demo")

	if !second.Created.Equal(first.Created) {
		t.Fatalf("created changed on upsert: %v -> %v", first.Created, second.Created)
	}
	if second.Secrets["password"] != "rotated" || second.Parameters["user"] != "admin" {
		t.Fatalf("upsert did not overwrite payload: %+v", second)
	}
	if second.Updated.Before(first.Updated) {
		t.Fatal("updated went backwards on upsert")
	}
}

func TestSQLite_WrongKey_FailsDecrypt(t *testing.T) {
	ctx := context.Background()
	ks := NewKeyStore(testKey(0x11))
	s := openTestStore(t, ks)
	_ = s.PutSpec(ctx, sampleSpec())
	ks.Store(testKey(0x22)) // rotate to the wrong key
	if _, err := s.GetSpec(ctx, "h1", "postgres", "demo"); err == nil {
		t.Fatal("GetSpec with wrong key should fail, not panic")
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/store/ -run TestSQLite -v`
Expected: FAIL — `undefined: OpenSQLite` / `undefined: SQLite`.

- [ ] **Step 4: Write the implementation**

`internal/store/sqlite.go`:

```go
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	_ "modernc.org/sqlite"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS specs (
  host       TEXT NOT NULL,
  template   TEXT NOT NULL,
  slug       TEXT NOT NULL,
  parameters TEXT NOT NULL,
  secrets    BLOB NOT NULL,
  created    INTEGER NOT NULL,
  updated    INTEGER NOT NULL,
  PRIMARY KEY (host, template, slug)
);`

// SQLite is the durable Store backed by a single SQLite file. Secrets are
// sealed with the key held in keys, read fresh on every Put/Get so a SIGHUP
// key swap takes effect immediately.
type SQLite struct {
	db   *sql.DB
	keys *KeyStore
}

// OpenSQLite opens (creating if needed) the SQLite file at path and ensures the
// schema exists. keys supplies the AES-256-GCM secret key.
func OpenSQLite(path string, keys *KeyStore) (*SQLite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// SQLite is single-writer; cap the pool to one connection to avoid
	// "database is locked" under concurrent Apply/Delete.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA user_version = 1`); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &SQLite{db: db, keys: keys}, nil
}

// Close releases the underlying database handle.
func (s *SQLite) Close() error { return s.db.Close() }

func (s *SQLite) PutSpec(ctx context.Context, sp Spec) error {
	params, err := json.Marshal(sp.Parameters)
	if err != nil {
		return err
	}
	secJSON, err := json.Marshal(sp.Secrets)
	if err != nil {
		return err
	}
	key := s.keys.Load()
	blob, err := seal(key, secJSON)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	_, err = s.db.ExecContext(ctx, `
INSERT INTO specs (host, template, slug, parameters, secrets, created, updated)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(host, template, slug) DO UPDATE SET
  parameters = excluded.parameters,
  secrets    = excluded.secrets,
  updated    = excluded.updated`,
		sp.Host, sp.Template, sp.Slug, string(params), blob, now, now)
	return err
}

func (s *SQLite) GetSpec(ctx context.Context, host, template, slug string) (Spec, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT parameters, secrets, created, updated FROM specs WHERE host=? AND template=? AND slug=?`,
		host, template, slug)
	var (
		paramsJSON       string
		blob             []byte
		created, updated int64
	)
	if err := row.Scan(&paramsJSON, &blob, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Spec{}, ErrNotFound
		}
		return Spec{}, err
	}
	var params map[string]any
	if err := json.Unmarshal([]byte(paramsJSON), &params); err != nil {
		return Spec{}, err
	}
	secJSON, err := open(s.keys.Load(), blob)
	if err != nil {
		return Spec{}, err
	}
	var secrets map[string]string
	if err := json.Unmarshal(secJSON, &secrets); err != nil {
		return Spec{}, err
	}
	return Spec{
		Host: host, Template: template, Slug: slug,
		Parameters: params, Secrets: secrets,
		Created: time.Unix(created, 0), Updated: time.Unix(updated, 0),
	}, nil
}

func (s *SQLite) DeleteSpec(ctx context.Context, host, template, slug string) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM specs WHERE host=? AND template=? AND slug=?`, host, template, slug)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/store/ -v`
Expected: PASS (all store tests, including the 5 SQLite ones).

- [ ] **Step 6: Verify it compiles under the full build tags too**

Run: `go build -tags "$TAGS" ./internal/store/`
Expected: no output (success).

- [ ] **Step 7: Commit**

```bash
gofmt -w internal/store/sqlite.go internal/store/sqlite_test.go
git add internal/store/sqlite.go internal/store/sqlite_test.go go.mod go.sum
git commit -m "feat(store): SQLite-backed spec store with encrypted secrets (#31)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Wire the store into `instance.Service`

**Files:**
- Modify: `internal/instance/service.go` (struct, imports, `SetStore`, Apply, Delete)
- Test: `internal/instance/service_test.go` (append new tests)

All tests here import podman → run with `-tags "$TAGS"`.

- [ ] **Step 1: Write the failing tests** (append to `internal/instance/service_test.go`)

```go
func TestService_Apply_PersistsSpec(t *testing.T) {
	svc, _ := newSvc(t)
	mem := store.NewMemory()
	svc.SetStore(mem)
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	sp, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.Equal(t, "p", sp.Secrets["password"])
	assert.Equal(t, "docker.io/library/postgres:16", sp.Parameters["image"])
}

func TestService_Apply_PlayKubeFail_NoSpec(t *testing.T) {
	svc, f := newSvc(t)
	mem := store.NewMemory()
	svc.SetStore(mem)
	f.PlayKubeErr = errors.New("boom")
	ctx := context.Background()

	require.Error(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	_, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestService_Apply_StorePutError_Fatal(t *testing.T) {
	svc, _ := newSvc(t)
	mem := store.NewMemory()
	mem.PutErr = errors.New("db down")
	svc.SetStore(mem)

	err := svc.Apply(context.Background(), "h1", pgApply("demo"), ApplyOptions{Replace: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist spec")
}

func TestService_Delete_RemovesSpec(t *testing.T) {
	svc, _ := newSvc(t)
	mem := store.NewMemory()
	svc.SetStore(mem)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "demo", DeleteOptions{}))

	_, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestService_Delete_NilStore_OK(t *testing.T) {
	svc, _ := newSvc(t) // no SetStore
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "demo", DeleteOptions{}))
}
```

Add the store import to the test file's import block:
```go
	"github.com/iotready/podman-api/internal/store"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags "$TAGS" ./internal/instance/ -run 'TestService_Apply_Persists|TestService_Apply_PlayKubeFail|TestService_Apply_StorePutError|TestService_Delete_RemovesSpec|TestService_Delete_NilStore' -v`
Expected: FAIL — `svc.SetStore undefined`.

- [ ] **Step 3: Add the store field, import, and setter** (`internal/instance/service.go`)

Add to the import block:
```go
	"maps"
```
and:
```go
	"github.com/iotready/podman-api/internal/store"
```

Add a field to the `Service` struct (after `secretEnvs`):
```go
	store store.Store // nil = store disabled (stateless proxy behaviour)
```

Add the setter next to `SetHosts`:
```go
// SetStore wires the optional desired-state store. A nil store (the default)
// disables persistence and the daemon behaves as a stateless proxy. Called by
// main after construction, mirroring SetHosts.
func (s *Service) SetStore(st store.Store) { s.store = st }
```

- [ ] **Step 4: Persist on Apply** (`internal/instance/service.go`, in `Apply`)

Just before the existing "Push per-instance secrets, then zero the local copies." loop (the `for k, v := range req.Secrets` at line ~188), snapshot the secrets:
```go
	// Snapshot secrets before they are zeroed below; the store needs the
	// plaintext to persist (encrypted) for later migrate.
	var secretsCopy map[string]string
	if s.store != nil {
		secretsCopy = maps.Clone(req.Secrets)
	}
```

Then replace the final `return nil` of `Apply` (right after the successful `PlayKube`) with:
```go
	if s.store != nil {
		sp := store.Spec{
			Host:       host,
			Template:   req.Template,
			Slug:       req.Slug,
			Parameters: req.Parameters,
			Secrets:    secretsCopy,
		}
		if err := s.store.PutSpec(ctx, sp); err != nil {
			return fmt.Errorf("persist spec: %w", err)
		}
	}
	return nil
```

- [ ] **Step 5: Remove on Delete** (`internal/instance/service.go`, in `Delete`)

Immediately after the `opts.PruneVolumes` block and **before** the `if !podExisted && ...` not-found check, add:
```go
	if s.store != nil {
		if err := s.store.DeleteSpec(ctx, host, tmpl, slug); err != nil && !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("delete spec: %w", err)
		}
	}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test -tags "$TAGS" ./internal/instance/ -v`
Expected: PASS — the five new tests plus all pre-existing instance tests still green.

- [ ] **Step 7: Commit**

```bash
gofmt -w internal/instance/service.go internal/instance/service_test.go
git add internal/instance/service.go internal/instance/service_test.go
git commit -m "feat(instance): persist spec on Apply, remove on Delete via store (#31)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Flags, startup wiring, and SIGHUP key reload

**Files:**
- Modify: `cmd/podman-api/main.go`
- Create: `cmd/podman-api/main_test.go`

Package `main` imports podman → tests run with `-tags "$TAGS"`.

- [ ] **Step 1: Write the failing tests** (`cmd/podman-api/main_test.go`)

```go
package main

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

func writeKey(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(i)
	}
	p := filepath.Join(t.TempDir(), "spec.key")
	if err := os.WriteFile(p, []byte(base64.StdEncoding.EncodeToString(raw)), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestOpenStore_Disabled(t *testing.T) {
	st, ks, err := openStore("", "")
	if err != nil || st != nil || ks != nil {
		t.Fatalf("disabled store should be (nil,nil,nil), got (%v,%v,%v)", st, ks, err)
	}
}

func TestOpenStore_MissingKey(t *testing.T) {
	db := filepath.Join(t.TempDir(), "state.db")
	if _, _, err := openStore(db, ""); err == nil {
		t.Fatal("expected error when -state-db set without -spec-key-file")
	}
}

func TestOpenStore_BadKey(t *testing.T) {
	db := filepath.Join(t.TempDir(), "state.db")
	bad := filepath.Join(t.TempDir(), "bad.key")
	_ = os.WriteFile(bad, []byte("not-32-bytes"), 0o600)
	if _, _, err := openStore(db, bad); err == nil {
		t.Fatal("expected error for invalid key file")
	}
}

func TestOpenStore_Enabled(t *testing.T) {
	db := filepath.Join(t.TempDir(), "state.db")
	st, ks, err := openStore(db, writeKey(t))
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	if st == nil || ks == nil {
		t.Fatal("enabled store should return non-nil store and keystore")
	}
	// Sanity: the returned store actually works end-to-end.
	if err := st.PutSpec(context.Background(), storeSpecFixture()); err != nil {
		t.Fatalf("PutSpec via returned store: %v", err)
	}
}
```

Add this helper at the bottom of `main_test.go` (kept local to avoid importing test fixtures across packages):
```go
func storeSpecFixture() store.Spec {
	return store.Spec{
		Host: "h1", Template: "postgres", Slug: "demo",
		Parameters: map[string]any{"image": "postgres:16"},
		Secrets:    map[string]string{"password": "p"},
	}
}
```
and the import `"github.com/iotready/podman-api/internal/store"`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags "$TAGS" ./cmd/podman-api/ -run TestOpenStore -v`
Expected: FAIL — `undefined: openStore`.

- [ ] **Step 3: Add the `openStore` helper** (`cmd/podman-api/main.go`)

Add the import `"github.com/iotready/podman-api/internal/store"`, then add this function (next to `loadKeys`):
```go
// openStore wires the optional desired-state store from the two flags. It
// returns (nil, nil, nil) when stateDB is empty (store disabled). When stateDB
// is set it requires a readable, valid key file; any problem is an error so the
// caller can refuse to start.
func openStore(stateDB, keyFile string) (store.Store, *store.KeyStore, error) {
	if stateDB == "" {
		return nil, nil, nil
	}
	if keyFile == "" {
		return nil, nil, fmt.Errorf("-state-db requires -spec-key-file")
	}
	key, err := store.LoadKeyFile(keyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("spec key: %w", err)
	}
	ks := store.NewKeyStore(key)
	st, err := store.OpenSQLite(stateDB, ks)
	if err != nil {
		return nil, nil, fmt.Errorf("state db: %w", err)
	}
	return st, ks, nil
}
```

- [ ] **Step 4: Declare the flags and wire startup** (`cmd/podman-api/main.go`, in `main`)

Add to the flag block (after `auditLogFile`):
```go
		stateDB     = flag.String("state-db", "", "if set, enable the desired-state store at this SQLite path (required for migrate/evacuate)")
		specKeyFile = flag.String("spec-key-file", "", "path to the 32-byte secret encryption key (required when -state-db is set)")
```

After `svc := instance.NewService(client, hosts, tmpls)` (line ~77), add:
```go
	specStore, specKeys, err := openStore(*stateDB, *specKeyFile)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	if specStore != nil {
		svc.SetStore(specStore)
		log.Printf("desired-state store enabled: %s", *stateDB)
	}
```

- [ ] **Step 5: Add SIGHUP key reload** (`cmd/podman-api/main.go`, inside the existing `for range hup` loop)

After the hosts-reload block (after line ~152, still inside the loop), add:
```go
			if specKeys != nil {
				if newKey, err := store.LoadKeyFile(*specKeyFile); err != nil {
					log.Printf("spec key reload FAILED, keeping previous key: %v", err)
				} else {
					specKeys.Store(newKey)
					log.Printf("spec key reloaded from %s", *specKeyFile)
				}
			}
```

- [ ] **Step 6: Run tests + build to verify**

Run:
```bash
go test -tags "$TAGS" ./cmd/podman-api/ -run TestOpenStore -v
go build -tags "$TAGS" -o /tmp/podman-api-build ./cmd/podman-api
```
Expected: tests PASS (4); build succeeds (no output).

- [ ] **Step 7: Smoke-test the refuse-to-start and enabled paths**

Run:
```bash
# refuses to start: state-db without key
/tmp/podman-api-build -state-db /tmp/s.db -hosts-dir /tmp/nohosts 2>&1 | grep -q "store:" && echo "REFUSED-OK"
```
Expected: prints `REFUSED-OK` (it fails on the missing key before the hosts error — if hosts error wins, instead pass a valid empty hosts dir: `mkdir -p /tmp/emptyhosts` and use `-hosts-dir /tmp/emptyhosts`, expecting the `store:` fatal).

- [ ] **Step 8: Commit**

```bash
gofmt -w cmd/podman-api/main.go cmd/podman-api/main_test.go
git add cmd/podman-api/main.go cmd/podman-api/main_test.go
git commit -m "feat(cmd): -state-db/-spec-key-file flags + SIGHUP key reload (#31)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Document the flags

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Find the flags/configuration section**

Run: `grep -n "audit-log-file\|hosts-dir\|## " README.md | head -40`
Expected: locate the flag list / configuration table the existing flags live in.

- [ ] **Step 2: Add the two flags and a key-generation note**

In the same place the existing flags are documented, add entries for `-state-db` and `-spec-key-file`, plus a short note. Use the surrounding format (table row or list item — match what's there). Content to include:

> - `-state-db <path>` — enable the desired-state store (SQLite) at this path. Off by default; required for `migrate`/`evacuate`. When set, the daemon persists each instance's parameters and **encrypted** secrets so it can later move instances by name.
> - `-spec-key-file <path>` — 32-byte AES-256-GCM key protecting stored secrets. Required when `-state-db` is set; the daemon refuses to start without a readable, valid key. Keep it `0600` and **separate from the database** — compromise of both the DB and the key is required to leak secrets. SIGHUP reloads it.
>
> Generate a key:
> ```sh
> head -c 32 /dev/urandom | base64 > spec.key && chmod 600 spec.key
> ```
> Note: rotating the key does not re-encrypt existing rows — secrets written under the old key become unreadable.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(readme): document -state-db/-spec-key-file flags (#31)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification (after all tasks)

- [ ] **Full test suite with tags:**
  ```bash
  go test -tags "$TAGS" ./...
  ```
  Expected: all packages PASS.

- [ ] **gofmt + vet clean:**
  ```bash
  gofmt -l .              # expect empty output
  go vet -tags "$TAGS" ./...
  ```

- [ ] **Build the binary:**
  ```bash
  make build
  ```
  Expected: `bin/podman-api` produced.

- [ ] **Confirm store-disabled is the default:** the binary with no `-state-db` starts and behaves exactly as today (no DB file created, no key required).

Then use **superpowers:finishing-a-development-branch** to open the PR against `main` (one PR for #31), and check off the Phase 2 box on the #29 tracker once merged.
