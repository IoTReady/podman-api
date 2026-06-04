# #54 quick-wins bundle — design

**Issue:** #54 (migrate/evacuate hardening backlog, umbrella). This bundle closes
three small, independent sub-items; the umbrella stays open for the rest.

**Goal:** Make evacuate concurrency operator- and client-tunable, give a bad
evacuate map a single consistent HTTP status, and stop `ClaimNext` from
surfacing transient SQLite `BUSY`/`LOCKED` (which both reddens CI and spams prod
logs).

## Item 1 — `evacuateConcurrency`: flag + per-request override

Today `evacuate.Handler` reads a package-level `var evacuateConcurrency = 2`
(mutated by tests). There is no way for an operator or client to tune it.

**Changes:**

- `internal/evacuate/handler.go`
  - Remove the package `var evacuateConcurrency`.
  - Add `const defaultEvacuateConcurrency = 2` and `const maxEvacuateConcurrency = 32`.
  - Add field `Concurrency int` to `Handler` (0 → use default).
  - In `Run`, compute the effective bound:
    1. start from `defaultEvacuateConcurrency`
    2. if `h.Concurrency > 0`, use it
    3. if `req.Concurrency > 0`, it overrides (per-request wins)
    4. clamp to `[1, maxEvacuateConcurrency]`
  - Use the computed value for the `sem` channel.
- `internal/instance/evacuate.go`
  - Add `Concurrency int \`json:"concurrency,omitempty"\`` to `EvacuateRequest`.
    It is request-only metadata: `ResolveEvacuation` ignores it (it does not
    affect the migrate plan), and it is carried through the job args so the
    handler sees it at execution time.
- `cmd/podman-api/main.go`
  - Add flag `evacuateConcurrency = flag.Int("evacuate-concurrency", 2, "max child migrations an evacuate runs at once (1..32); a request's \"concurrency\" overrides per call")`.
  - Wire it: `&evacuate.Handler{Svc: svc, Jobs: db, Concurrency: *evacuateConcurrency}`.
- `internal/evacuate/handler_test.go`
  - `TestEvacuateConcurrencyBound` no longer mutates the removed package var; it
    sets `h.Concurrency = 1` instead. Add coverage that a per-request
    `Concurrency` overrides the handler default and that out-of-range values
    clamp.

**Clamp rationale:** an unauthenticated-shaped body field that spawns goroutines
must be bounded; `maxEvacuateConcurrency = 32` is well above any real host's
instance count while preventing a fat-fingered `"concurrency": 100000`.

## Item 2 — one consistent status for a bad evacuate map

Today `ResolveEvacuation` returns bare `ErrUnknownHost` (→ **404**) when a
destination host named in `map` is unknown, but `ErrInvalidEvacuation` (→ **400**)
for unmapped / extra / ambiguous slugs. A single malformed map yields 400 or 404
depending on which check trips first.

**Rule:** any bad **map content** → 400 `invalid_request`. `from_host` not
existing stays **404** (it is the resource being operated on, not map content).

**Changes:**

- `internal/instance/evacuate.go`
  - The unknown-destination-host branch (currently `return nil, ErrUnknownHost`
    inside the `for slug, dest := range req.Map` loop) becomes:
    ```go
    return nil, fmt.Errorf("%w: no such destination host %q for slug %q",
        ErrInvalidEvacuation, dest, slug)
    ```
  - `from_host` unknown (top of function) is unchanged — still `ErrUnknownHost`.
  - `dest == req.FromHost` is unchanged — still `ErrSameHost` (already → 400).
- `internal/api/errors.go` — unchanged; `ErrInvalidEvacuation` already maps to
  `invalid_request` / 400.
- `internal/instance/evacuate_test.go`
  - The `"unknown dest host"` subtest now asserts `ErrorIs(err, ErrInvalidEvacuation)`.
- `internal/api/evacuate_test.go`
  - Add `TestEvacuate_API_UnknownDestHost_400` asserting 400 and that no job is
    enqueued.

## Item 3 — `retryBusy` for SQLite single-statement writes

`busy_timeout` is already 5000ms in the DSN, yet `modernc.org/sqlite` can still
return primary result code `SQLITE_BUSY` (5) or `SQLITE_LOCKED` (6) immediately
under write-write contention (the busy handler is not always invoked). In
production the runner logs the `ClaimNext` error and retries on the next poll —
benign but noisy. `TestSQLite_ClaimNext_NoDoubleClaim` treats any error as fatal,
so it is flaky under load.

**Change:** a small retry helper, applied to every single-statement write so the
hardening is uniform (not a test-only patch).

- `internal/store/sqlite.go`
  - Promote the blank import to named: `sqlite "modernc.org/sqlite"`.
  - Add:
    ```go
    // retryBusyTimeout bounds how long a write retries past a transient
    // SQLITE_BUSY/LOCKED before giving up. busy_timeout(5000) handles most
    // contention; this is a thin backstop for the cases modernc returns BUSY
    // without invoking the busy handler.
    const retryBusyTimeout = 2 * time.Second

    func isBusy(err error) bool {
        var se *sqlite.Error
        if !errors.As(err, &se) {
            return false
        }
        c := se.Code() & 0xff // strip extended bits
        return c == 5 /* SQLITE_BUSY */ || c == 6 /* SQLITE_LOCKED */
    }

    // retryBusy runs fn, retrying on a transient BUSY/LOCKED with short backoff
    // until retryBusyTimeout elapses or ctx is done. Non-busy errors return
    // immediately.
    func retryBusy(ctx context.Context, fn func() error) error {
        deadline := time.Now().Add(retryBusyTimeout)
        backoff := 1 * time.Millisecond
        for {
            err := fn()
            if err == nil || !isBusy(err) {
                return err
            }
            if time.Now().After(deadline) || ctx.Err() != nil {
                return err
            }
            select {
            case <-ctx.Done():
                return err
            case <-time.After(backoff):
            }
            if backoff < 50*time.Millisecond {
                backoff *= 2
            }
        }
    }
    ```
  - Wrap the write body of each single-statement write so a BUSY is retried:
    `ClaimNext`, `Enqueue`, `StartChild`, `Finish`, `FailRunning`, `AppendStep`,
    `PruneJobs`. Each keeps its existing logic inside the `retryBusy(ctx, func() error { ... })`
    closure; the closure assigns to the enclosing result vars.
  - `PutSpec` (specs table) is included for consistency.
  - Multi-statement transactions (none today beyond `PruneJobs`, which uses a
    single `BeginTx`) wrap the whole `BeginTx…Commit` unit so a retry restarts
    the transaction cleanly.

> **Time note:** `Date.now()`-style timestamps are unaffected — `retryBusy` uses
> `time.Now()` for its own deadline only; job timestamps are still stamped by the
> wrapped write the same as before.

- `internal/store/jobs_test.go`
  - `TestSQLite_ClaimNext_NoDoubleClaim` is unchanged in intent (no double-claim,
    all `n` claimed exactly once) and should now pass deterministically because
    `ClaimNext` swallows transient BUSY.
  - Add a focused unit test for `retryBusy`/`isBusy`: a fake `fn` returning a
    synthetic `*sqlite.Error{Code: SQLITE_BUSY}` twice then `nil` succeeds; a
    non-busy error returns immediately without retry.

## Testing

- `make test` green (race detector via existing CI tags).
- `gofmt -l .` empty, `go vet` clean.
- The previously-flaky `TestSQLite_ClaimNext_NoDoubleClaim` run repeatedly
  (`go test -run TestSQLite_ClaimNext_NoDoubleClaim -count=20`) stays green.

## Out of scope (remain on #54)

Volume-copy integrity, app-readiness probe, separate orchestration pool, job
cancellation, dry-run preview, auto-resume, per-host secret provisioning,
bind-mount support. Each is its own spec.
