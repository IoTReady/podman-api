# Phase 2: State store foundation — design

**Date:** 2026-06-03
**Status:** Approved (brainstorm)
**Tracking:** Forgejo #31 (part of milestone #29 / #40)
**Umbrella:** `docs/superpowers/specs/2026-06-03-migrate-evacuate-design.md`

## Goal

Give the daemon a durable, encrypted record of every instance's desired state
(`parameters` + `secrets`) so later phases can migrate/evacuate instances by
name. This phase adds the persistence layer only — **no new user-facing verbs**.
The store is opt-in; with it off, the daemon behaves exactly as today.

## Why this is load-bearing

Two facts (from the umbrella doc) make name-only migrate impossible without a
store:

1. **Secrets are write-only.** `podman.Secret` carries only `{Name, CreatedAt}`;
   Apply writes per-instance secrets to a host then zeroes its in-memory copy. It
   cannot read them back.
2. **The daemon is stateless about specs.** A running instance is just a labelled
   Pod; its original `parameters`/`secrets` live nowhere recoverable.

So we persist desired state on Apply and remove it on Delete.

## Decisions locked in this brainstorm

| Decision | Choice |
| --- | --- |
| SQLite driver | **`modernc.org/sqlite`** (pure-Go) — keeps store unit tests off the podman cgo chain |
| Store-write failure during Apply | **Fatal** — Apply returns error; idempotent retry converges |
| Store-write failure during Delete | **Fatal** (symmetry); `ErrNotFound` swallowed |
| Service wiring | **`SetStore` setter** mirroring `SetHosts` — `NewService` signature unchanged |
| Secrets encryption | Whole secrets map sealed as **one blob** (not per-key) |
| `ListSpecs` | **Deferred** to the consuming migrate/evacuate phase (YAGNI) |

## Package layout

New package `internal/store`, imported by `internal/instance`. `store.Spec` uses
only plain types (`map[string]any`, `map[string]string`, `time.Time`) so there is
no import cycle.

| File | Responsibility |
| --- | --- |
| `store.go` | `Store` interface, `Spec` struct, `ErrNotFound` |
| `crypto.go` | `seal`/`open` — AES-256-GCM, 12-byte random nonce prefixed to ciphertext |
| `key.go` | `KeyStore` (`atomic.Pointer[[32]byte]`, mirrors `auth.KeyStore`) + `LoadKeyFile` |
| `sqlite.go` | `OpenSQLite(path string, keys *KeyStore)`, schema init, Put/Get/Delete |
| `memory.go` | in-memory `Store` + error hook, for Service-layer unit tests |

### Interface

```go
package store

type Spec struct {
    Host, Template, Slug string
    Parameters map[string]any
    Secrets    map[string]string
    Created, Updated time.Time
}

type Store interface {
    PutSpec(ctx context.Context, s Spec) error
    GetSpec(ctx context.Context, host, template, slug string) (Spec, error)
    DeleteSpec(ctx context.Context, host, template, slug string) error
}

var ErrNotFound = errors.New("spec not found")
```

`ListSpecs(ctx, host)` is intentionally omitted — nothing in this phase consumes
it. It is added in the phase that needs it (migrate/evacuate).

## Schema & encryption

```sql
CREATE TABLE IF NOT EXISTS specs (
  host     TEXT NOT NULL,
  template TEXT NOT NULL,
  slug     TEXT NOT NULL,
  parameters TEXT NOT NULL,   -- JSON of map[string]any
  secrets    BLOB NOT NULL,   -- nonce || AES-256-GCM ciphertext of JSON(map[string]string)
  created INTEGER NOT NULL,   -- unix seconds
  updated INTEGER NOT NULL,   -- unix seconds
  PRIMARY KEY (host, template, slug)
);
-- PRAGMA user_version = 1   (reserved for future migrations)
```

`OpenSQLite` creates the table if absent and sets `user_version`.

**Upsert** (`PutSpec`):

```sql
INSERT INTO specs (host,template,slug,parameters,secrets,created,updated)
VALUES (?,?,?,?,?,?,?)
ON CONFLICT(host,template,slug) DO UPDATE SET
  parameters = excluded.parameters,
  secrets    = excluded.secrets,
  updated    = excluded.updated;
```

`created` is **not** in the update set, so Apply-with-Replace (secret/param
rotation) preserves the original birth time and bumps only `updated`.

**Encryption.** `seal(key [32]byte, plaintext []byte) ([]byte, error)` returns
`nonce(12) || AES-256-GCM ciphertext`; `open` reverses it. The plaintext is
`json.Marshal(spec.Secrets)`. The whole map is one blob — simpler than per-key
columns and sufficient. `sqlite.go` reads the current key from the injected
`*KeyStore` on **every** seal/open, so the seam is ready for a future
re-encrypting rotation; the key itself is loaded once at startup (no runtime
swap — see *Key handling*).

**Key-rotation caveat.** Rotating the key file does **not** re-encrypt existing
rows; ciphertext written under the old key becomes unreadable. Automatic
re-encryption is out of scope for this phase. Documented, not coded around.

## Key handling

`LoadKeyFile(path string) ([32]byte, error)`:

- Reads the file, trims trailing whitespace/newline.
- If the result is exactly 32 bytes → use as the raw key.
- Else base64-decode (std encoding); if that yields exactly 32 bytes → use it.
- Anything else → error.

`KeyStore` mirrors `internal/auth.KeyStore` exactly:

```go
type KeyStore struct{ key atomic.Pointer[[32]byte] }
func NewKeyStore(k [32]byte) *KeyStore
func (s *KeyStore) Store(k [32]byte)
func (s *KeyStore) Load() [32]byte
```

The key file is expected to be `0600`, kept separate from the DB, and is **never
logged**.

## Service integration

`instance.Service` gains a nil-able field and a setter:

```go
type Service struct {
    // ...existing fields...
    store store.Store // nil = store disabled
}

func (s *Service) SetStore(st store.Store) { s.store = st }
```

`NewService`'s signature is unchanged; `main` calls `svc.SetStore(...)` after
construction, exactly as it already does for hosts. Nil store ⇒ today's behavior.

**Apply** (`internal/instance/service.go`, around lines 187–204): the existing
flow writes per-instance secrets to the host, then zeroes `req.Secrets`, then
calls `PlayKube`. Change:

1. Before the zero loop, snapshot the secrets: `secretsCopy := maps.Clone(req.Secrets)`.
2. After `PlayKube` returns success, if `s.store != nil`, build a `store.Spec`
   (`Host`, `Template`, `Slug`, `Parameters: req.Parameters`, `Secrets: secretsCopy`)
   and call `s.store.PutSpec(ctx, spec)`.
3. A non-nil error from `PutSpec` → Apply returns that error (wrapped). The
   instance is running, but Apply is idempotent (re-play is a no-op, spec
   re-written), so the client's retry converges and the success contract holds:
   **if Apply reports success, the instance is migratable.**

Persisting *after* `PlayKube` (not before) means a failed play never leaves a
spec for a non-existent instance.

**Delete** (`internal/instance/service.go`): after the pod is successfully
removed/pruned, if `s.store != nil`, call `s.store.DeleteSpec(ctx, host, tmpl,
slug)`. `store.ErrNotFound` is swallowed (idempotent); any other error is fatal
to Delete (returns error). Same symmetry/idempotency argument as Apply.

## Flags & startup

Two new flags in `cmd/podman-api/main.go`:

- `-state-db <path>` — path to the SQLite file. Empty (default) = store disabled.
- `-spec-key-file <path>` — path to the 32-byte encryption key.

Startup wiring (only when `-state-db` is non-empty):

1. `LoadKeyFile(*specKeyFile)` — on error, **the daemon refuses to start** (fatal
   log + non-zero exit). This also fires if `-state-db` is set but
   `-spec-key-file` is empty/unreadable/wrong length.
2. `keyStore := store.NewKeyStore(key)`.
3. `st, err := store.OpenSQLite(*stateDB, keyStore)` — fatal on error.
4. `svc.SetStore(st)`.

The spec key is loaded **once at startup** — there is deliberately no SIGHUP
hot-reload (unlike the auth keys). Because rows are not re-encrypted, swapping to
a different key at runtime would silently make existing rows undecryptable; a
restart covers the only safe case (fixing a startup typo before rows exist). The
`*KeyStore` seam is retained so a future re-encrypting rotation can use it.
*(Revised after PR #40 review — issue #41; the original design mirrored the
auth-key hot-reload, which is unsafe for a data-encryption key.)*

## Threat model (unchanged from umbrella)

Enabling the store means the daemon host holds **encrypted** tenant secrets. DB
compromise **and** key compromise together = leak; today neither exists. This is
the explicit, opt-in cost of name-only migrate. Mitigations: key file `0600`,
separate from the DB; key never logged; the `KeyStore`/`seal`/`open` seam leaves
room for a future KMS/age backend.

## Testing (TDD)

**`crypto.go`**
- `seal` then `open` round-trips to the original plaintext.
- Ciphertext ≠ plaintext.
- `open` with a different key fails (GCM auth error).
- Two `seal`s of the same plaintext differ (nonce uniqueness).

**`key.go`**
- Raw 32-byte file → key loads.
- Base64-of-32-bytes file (with trailing newline) → key loads.
- Wrong length (raw or decoded) → error.
- Missing file → error.
- `KeyStore.Store` then `Load` returns the latest key.

**`sqlite.go`**
- `PutSpec` then `GetSpec` round-trips, including secrets decrypted back to the
  original map.
- `GetSpec` for an absent row → `ErrNotFound`.
- `DeleteSpec` removes the row (subsequent `GetSpec` → `ErrNotFound`).
- `DeleteSpec` on an absent row → `ErrNotFound` (caller decides to swallow).
- Upsert: a second `PutSpec` for the same PK preserves `created`, bumps `updated`,
  and overwrites parameters/secrets.
- `GetSpec` with a `KeyStore` holding the wrong key → decrypt error (not a panic).

**`internal/instance` (Service, white-box, fake podman client + `store.Memory`)**
- Apply with a store set persists the spec; a follow-up `GetSpec` returns the
  parameters and the decrypted secrets.
- Apply where `PlayKube` fails writes **no** spec.
- Apply where the store's `PutSpec` errors (error hook) → Apply returns an error.
- Delete removes the spec; Delete when the spec is absent does not error on that
  account.
- Store disabled (nil): Apply/Delete behave exactly as today, no panic.

`store.Memory` is an in-memory `Store` with a settable `PutErr`/`DeleteErr` hook,
mirroring the `internal/podman/fake` pattern, so the Service layer is unit-testable
without touching SQLite or a key file.

## Build / dependency notes

- Add `modernc.org/sqlite` as a direct dependency (`go get`). It is pure-Go, so
  `store` package unit tests run under the standard build without the
  `containers_image_openpgp …` tags. The full binary still builds with the
  existing Makefile tags.
- Keep `go.mod`/`go.sum` tidy (`go mod tidy` with the build tags, per CLAUDE.md).

## Out of scope for this phase

- No `migrate`/`evacuate`/`jobs` verbs (later phases).
- No `ListSpecs` (added when first consumed).
- No automatic key rotation / re-encryption of existing rows.
- No backfill of specs for instances created before the store existed (the
  documented adoption path is a one-time re-`Apply`; see umbrella "Legacy
  adoption").
