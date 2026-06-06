# #113 — Coherent recoverable key-fault handling in reconcile

**Issue:** tej/podman-api#113 (follow-up from #92 / PR #112 review).

**Goal:** Make the two static key-file misconfigurations — daemon started with **no**
key vs the **wrong** key — land on the same recoverable reconcile outcome, while
genuine plaintext-JSON corruption stays terminal.

## Problem

`internal/instance/reconcile.go` `destSpecState` classifies a `GetSpec` failure when
the destination pod is healthy. After #92 it produced four states:

- `nil` → `specPersisted` (roll forward)
- `store.ErrNotFound` → `specAbsent` (fall through)
- `store.ErrSpecCorrupt` → `specCorrupt` (terminal `failed`)
- everything else → `specInconclusive` (retry)

`store.ErrSecretsNeedKey` (returned when a spec row carries a secrets blob but the
store was opened **without** `-spec-key-file`) falls into the `default` arm and
retries every 30s forever. A **wrong** key file (operator typo) fails at the GCM
`open()` step, which #92 wrapped as `ErrSpecCorrupt` → terminal `failed`.

So the two key-file faults — both static configuration, both recoverable by a
restart with the correct key — land on opposite ends: no-key loops forever,
wrong-key kills the in-flight migrate.

## Decision

Treat both key faults as one **recoverable** state. (Chosen over "minimal: no-key
only" and "coherent: both terminal" — see issue thread.)

Rationale: a missing/wrong key is recoverable by an operator restart without losing
the migrate intent. A distinct, visible reconcile step signals that the
*configuration* is the problem. Genuine ciphertext/JSON corruption is far rarer than
a key misconfiguration; erring toward not-destroying a recoverable migrate is the
right default. This revisits #92's "decrypt = terminal" trade-off, which the #92
spec recorded as "accepted for now" and the #113 comment nominated for reconciliation
here.

## Design

### Store layer (`internal/store`)

`GetSpec`'s permanent-failure returns split by *cause*:

| Cause in `GetSpec` | Sentinel | Meaning |
|---|---|---|
| `s.keys == nil`, blob present | `ErrSecretsNeedKey` (existing) | no key configured |
| `open()` (GCM decrypt) fails | **`ErrSecretsUndecryptable`** (new) | wrong key **or** corrupt ciphertext — indistinguishable at GCM; recoverable by restart with correct key |
| `params` JSON unmarshal fails | `ErrSpecCorrupt` (existing) | plaintext column corrupt |
| post-decrypt `secrets` JSON unmarshal fails | `ErrSpecCorrupt` | decrypted cleanly but plaintext malformed → genuine corruption |
| `domains` JSON unmarshal fails | `ErrSpecCorrupt` | plaintext column corrupt |

Only the `open()` path changes sentinel (was `ErrSpecCorrupt`, now
`ErrSecretsUndecryptable`). The post-decrypt secrets-unmarshal stays `ErrSpecCorrupt`
because a successful `open()` means GCM authenticated the plaintext: malformed JSON
there is data corruption, not a key fault.

New sentinel doc: distinct from `ErrSpecCorrupt` — recoverable by restarting with the
correct `-spec-key-file`; the row itself may be intact.

### Reconcile (`internal/instance/reconcile.go`)

- Add `specState` value `specNeedsKey`.
- `destSpecState`: `case errors.Is(err, store.ErrSecretsNeedKey), errors.Is(err,
  store.ErrSecretsUndecryptable): return specNeedsKey`.
- Caller (the `ds == destHealthy` block), alongside `specInconclusive`: on
  `specNeedsKey` emit step `reconcile-needs-key` with detail "destination spec
  secrets unreadable (key missing or wrong); restart with -spec-key-file" and return
  `false, false, "", nil` (inconclusive → retry). Neither host is mutated.
- `specCorrupt` (JSON) path unchanged → terminal `failed`.

### Necessary ripple (preserve current synchronous behavior)

Splitting the sentinel changes the error a wrong-key decrypt returns to synchronous
callers; without these updates they would regress from 422/degrade to 500/no-degrade.

- `internal/ui/render.go` `errorStatus`: map `ErrSecretsUndecryptable` → 422 (beside
  `ErrSpecCorrupt`).
- `internal/api/errors.go` `classify`: map `ErrSecretsUndecryptable` → a code +
  422 (beside `ErrSpecCorrupt`).
- `internal/ui/handlers_secrets.go` `secretsFormData`: degrade (Corrupt banner, nil
  err) on `ErrSecretsUndecryptable` too.

## Tests

- `internal/store/sqlite_test.go`:
  - Update the wrong-key test to assert `errors.Is(err, ErrSecretsUndecryptable)`
    **and** `!errors.Is(err, ErrSpecCorrupt)`.
  - Keep `CorruptSecretsJSON` (seals `{bad` under the live key) asserting
    `ErrSpecCorrupt`.
- `internal/instance/reconcile_test.go`:
  - Keep `..._DestSpecCorrupt_Terminal` (`ErrSpecCorrupt` → resolved/!ok terminal).
  - Add `..._DestSpecNeedsKey_Inconclusive` covering both `ErrSecretsNeedKey` and
    `ErrSecretsUndecryptable`: resolved=false, ok=false, neither pod mutated.
- `internal/ui` / `internal/api`: a test asserting `ErrSecretsUndecryptable` maps to
  422 / degrades, mirroring the existing `ErrSpecCorrupt` coverage.

## Out of scope

- The `TODO(#54)` explicit-commit-marker redesign.
- #114 (the RMW race) — separate PR.
