# Store/Reconcile Consistency Follow-ups (#117) — Design

**Status:** accepted

Two non-blocking follow-ups surfaced in the review of PR #115 (#113). Both are
pre-existing and were out of #113's reconcile-coherence scope. Refs: PR #115
review notes 1 & 3.

## 1. `GetHostSecret`: typed undecryptable error

### Problem

`internal/store/sqlite.go` `GetHostSecret` ends with `return open(s.keys.Load(), blob)`,
so a **wrong-key** decrypt failure surfaces as a raw GCM error → HTTP 500. After
#113 the *spec* secrets path returns the typed `store.ErrSecretsUndecryptable`
→ 422. The two key-fault paths should be coherent.

### Resolution

Wrap the final `open()` failure in `ErrSecretsUndecryptable`, exactly as #113 did
for `GetSpec`'s secrets blob:

```go
val, err := open(s.keys.Load(), blob)
if err != nil {
    return nil, fmt.Errorf("%w: decrypt host secret: %v", ErrSecretsUndecryptable, err)
}
return val, nil
```

The `s.keys == nil` → `ErrSecretsNeedKey` and `sql.ErrNoRows` → `ErrNotFound`
branches are unchanged.

### Caller safety (verified)

`GetHostSecret` has two callers, both in the migrate/evacuate host-secret
re-provisioning path:

- `internal/instance/migrate.go:119` (`hostSecretProvisionable`) — branches on
  `ErrNotFound`, funnels everything else through `default` (returns the error as
  an inconclusive infra failure).
- `internal/instance/migrate.go:293` (executor provisioning) — branches on
  `ErrNotFound`, wraps everything else as `load host secret %q: %w`.

Neither does an `errors.Is`/branch on the raw GCM error, so wrapping it in
`ErrSecretsUndecryptable` changes only the *type* of the already-propagated
error, not control flow. A wrong-key host-secret read now classifies as 422 at
the API/UI boundary (via the existing `classify`/`errorStatus` handling added in
#113) instead of 500.

No new sentinel is introduced — `ErrSecretsUndecryptable` already exists and is
already mapped to 422 in `internal/api/errors.go` and `internal/ui/render.go`.

## 2. Reconcile step de-duplication

### Problem

`reconcile-needs-key` (and the pre-existing `reconcile-inconclusive`) call
`AppendStep` on every reconcile sweep. A daemon left misconfigured
(missing/wrong `-spec-key-file`) loops far longer than a transient host blip,
growing a job's step list unboundedly.

### Resolution: coalesce consecutive identical steps in the store layer

De-dup lives in `AppendStep` (both `sqlite.go` and `memory.go`), not in
reconcile. `AppendStep` already does the read-modify-write where the steps array
lives, it spans sweeps naturally (the array is persistent), and it helps *every*
reconcile loop — `reconcile-inconclusive` as well as `reconcile-needs-key` —
without each call site having to opt in. A reconcile-layer fix is awkward because
`JobContext` is recreated per sweep and would have to re-read the steps anyway.

**Semantics — coalesce with count + latest timestamp.** When an incoming step
has the same `Step` AND `Detail` as the current last element of the array:

- Do not append a new element.
- Bump the last element's occurrence count.
- Refresh the last element's `TS` to the incoming step's timestamp.

Otherwise append normally. This bounds the array (one row per *distinct
consecutive* step) while preserving a live "still looping, last attempt at T,
seen N times" signal an operator can act on. Coalescing is keyed on
*consecutive* identity only: an alternating `needs-key` / `inconclusive` churn
(e.g. a flapping host) stays visible as separate rows, which is meaningful.

### `JobStep` schema

Add a `Count` field holding the **total** occurrences, materialized only when
> 1 (so single occurrences keep the current JSON shape via `omitempty`):

```go
type JobStep struct {
    TS     time.Time `json:"ts"`
    Step   string    `json:"step"`
    Detail string    `json:"detail,omitempty"`
    Count  int       `json:"count,omitempty"` // total occurrences when coalesced (>1); 0/omitted ⇒ a single occurrence
}
```

`AppendStep` coalesce logic (identical in both stores):

```go
if n := len(arr); n > 0 && arr[n-1].Step == step.Step && arr[n-1].Detail == step.Detail {
    if arr[n-1].Count == 0 {
        arr[n-1].Count = 1 // normalize the implicit single occurrence
    }
    arr[n-1].Count++
    arr[n-1].TS = step.TS
} else {
    arr = append(arr, step)
}
```

### Surfacing the count

- `internal/api/jobs.go` `stepView`: add `Count int json:"count,omitempty"`,
  populate from `s.Count`.
- `internal/ui/templates/job-detail.html`: render the count when present —
  `<li>{{.Step}} {{.Detail}}{{if .Count}} (×{{.Count}}){{end}}</li>`.

## Out of scope

- No change to `ErrSecretsNeedKey` (`s.keys == nil`) handling.
- No cross-job or non-consecutive de-dup.
- No change to reconcile's retry cadence or terminal classification (that was
  #113's domain).

## Testing

- **Item 1:** a store test that seals a host secret under one key, rotates the
  keystore to a wrong key, and asserts `GetHostSecret` returns
  `ErrSecretsUndecryptable` (mirrors `TestSQLite_GetSpec_WrongKey_IsErrSecretsUndecryptable`).
- **Item 2:** `AppendStep` tests (sqlite + memory) asserting (a) two identical
  consecutive steps collapse to one row with `Count == 2` and the latest `TS`,
  (b) a differing `Detail` appends a new row, (c) `needs-key, inconclusive,
  needs-key` yields three rows (non-consecutive identicals do not coalesce).
- Full `make test` green; `gofmt -l` empty; `go vet` clean.
