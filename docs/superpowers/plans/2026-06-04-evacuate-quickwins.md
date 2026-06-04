# #54 quick-wins bundle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make evacuate concurrency tunable (flag + per-request override), give a bad evacuate map one consistent HTTP status (400), and add a `retryBusy` backstop so transient SQLite `BUSY`/`LOCKED` no longer surfaces from single-statement writes.

**Architecture:** Three independent changes across `internal/evacuate`, `internal/instance`, `internal/api`, `internal/store`, and `cmd/podman-api`. No new packages. Build/test with the remote-client tags via `make`.

**Tech Stack:** Go, `modernc.org/sqlite`, testify.

**Build/test note:** This repo's CGO drivers force the remote-client build tags. ALWAYS use `make build` / `make test` (never bare `go test ./...`). To run one package's tests with the right tags:
`go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/ -run TestName -v`

---

### Task 1: `retryBusy` backstop in the SQLite store

**Files:**
- Modify: `internal/store/sqlite.go`
- Test: `internal/store/jobs_test.go`

- [ ] **Step 1: Write the failing test for `isBusy` + `retryBusy`**

Add to `internal/store/jobs_test.go` (it already imports `context`, `testing`; add `errors` and `sqlite "modernc.org/sqlite"` to its import block):

```go
func TestRetryBusy(t *testing.T) {
	ctx := context.Background()

	// Non-busy error returns immediately, no retry.
	calls := 0
	want := errors.New("boom")
	if err := retryBusy(ctx, func() error { calls++; return want }); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
	if calls != 1 {
		t.Fatalf("non-busy error retried %d times, want 1 call", calls)
	}

	// Transient BUSY twice then success.
	calls = 0
	got := retryBusy(ctx, func() error {
		calls++
		if calls < 3 {
			return sqliteBusyErr()
		}
		return nil
	})
	if got != nil {
		t.Fatalf("retryBusy returned %v, want nil after retries", got)
	}
	if calls != 3 {
		t.Fatalf("retried %d times, want 3", calls)
	}
}
```

`*sqlite.Error`'s code is set via its constructor, not an exported field, so add a
tiny test helper in the same file that builds a BUSY error the same way the driver
does:

```go
// sqliteBusyErr builds an error that isBusy recognises as transient BUSY (5).
func sqliteBusyErr() error { return sqlite.NewError(5, "database is locked") }
```

> If `sqlite.NewError` does not exist in v1.51.0, fall back to a local type that
> satisfies `isBusy` by string match — but first check the package API (Step 2
> verifies the real signature).

- [ ] **Step 2: Verify the `modernc.org/sqlite` error API, then run the test to see it fail**

Confirm the constructor/code accessor names actually exported by v1.51.0:

Run: `go doc modernc.org/sqlite.Error` and `go doc modernc.org/sqlite | grep -i 'func New'`
Expected: shows `func (*Error) Code() int` and a constructor (e.g. `func NewError`). Adjust `isBusy`/`sqliteBusyErr` to the real names if they differ.

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/ -run TestRetryBusy -v`
Expected: FAIL — `undefined: retryBusy` / `undefined: isBusy`.

- [ ] **Step 3: Implement `isBusy` + `retryBusy`**

In `internal/store/sqlite.go`, change the blank driver import to named:

```go
	sqlite "modernc.org/sqlite"
```

(keep it in the import group; the driver still registers itself on import). Ensure `errors` and `time` are imported (both already are). Add near the top of the file, after the consts:

```go
// retryBusyTimeout bounds how long a write retries past a transient
// SQLITE_BUSY/LOCKED before giving up. busy_timeout(5000) handles most
// contention; this is a thin backstop for the cases modernc returns BUSY
// without invoking the busy handler.
const retryBusyTimeout = 2 * time.Second

// isBusy reports whether err is a transient SQLite BUSY/LOCKED (worth retrying).
func isBusy(err error) bool {
	var se *sqlite.Error
	if !errors.As(err, &se) {
		return false
	}
	switch se.Code() & 0xff { // strip extended result-code bits
	case 5, 6: // SQLITE_BUSY, SQLITE_LOCKED
		return true
	default:
		return false
	}
}

// retryBusy runs fn, retrying on a transient BUSY/LOCKED with short backoff
// until retryBusyTimeout elapses or ctx is done. Non-busy errors (and success)
// return immediately.
func retryBusy(ctx context.Context, fn func() error) error {
	deadline := time.Now().Add(retryBusyTimeout)
	backoff := time.Millisecond
	for {
		err := fn()
		if err == nil || !isBusy(err) {
			return err
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
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

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/ -run TestRetryBusy -v`
Expected: PASS.

- [ ] **Step 5: Wrap the single-statement writes with `retryBusy`**

Wrap the body of each of these methods in `internal/store/sqlite.go` so the DB call retries on BUSY: `ClaimNext`, `Enqueue`, `StartChild`, `Finish`, `FailRunning`, `AppendStep`, `PruneJobs`, `PutSpec`. Pattern — keep existing logic inside the closure, assign to enclosing result vars. Example for `ClaimNext`:

```go
func (s *SQLite) ClaimNext(ctx context.Context) (Job, bool, error) {
	var (
		j   Job
		ok  bool
	)
	err := retryBusy(ctx, func() error {
		now := time.Now().UnixNano()
		row := s.db.QueryRowContext(ctx, `
UPDATE jobs SET state='running', started=?
WHERE id = (SELECT id FROM jobs WHERE state='queued' ORDER BY created, id LIMIT 1)
  AND state='queued'
RETURNING `+jobColumns, now)
		jj, e := scanJob(row)
		if errors.Is(e, ErrNotFound) {
			j, ok = Job{}, false
			return nil
		}
		if e != nil {
			return e
		}
		j, ok = jj, true
		return nil
	})
	return j, ok, err
}
```

For `PruneJobs` wrap the whole `BeginTx … Commit` block inside the closure so a retry restarts the transaction cleanly (a partially-applied tx is rolled back by its existing `defer tx.Rollback()`). For the others, wrap the existing `ExecContext`/`QueryRowContext` body the same way. Do not change any SQL or timestamp logic — only move it inside the closure.

- [ ] **Step 6: Run the full store package tests (with race)**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -race ./internal/store/ -count=1`
Expected: PASS, including `TestSQLite_ClaimNext_NoDoubleClaim`.

- [ ] **Step 7: Stress the previously-flaky test**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -race ./internal/store/ -run TestSQLite_ClaimNext_NoDoubleClaim -count=20`
Expected: PASS 20/20 (was intermittently failing with SQLITE_BUSY).

- [ ] **Step 8: gofmt + vet + commit**

Run: `gofmt -l internal/store/ ; go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/`
Expected: no output / clean.

```bash
git add internal/store/sqlite.go internal/store/jobs_test.go
git commit -m "fix(store): retry transient SQLite BUSY/LOCKED on single-statement writes (#54)"
```

---

### Task 2: consistent 400 for a bad evacuate map

**Files:**
- Modify: `internal/instance/evacuate.go:75-77` (unknown-dest-host branch)
- Test: `internal/instance/evacuate_test.go` (`"unknown dest host"` subtest)
- Test: `internal/api/evacuate_test.go` (new API test)

- [ ] **Step 1: Update the instance-level test to expect `ErrInvalidEvacuation`**

In `internal/instance/evacuate_test.go`, the `"unknown dest host"` subtest currently asserts `ErrUnknownHost`. Change its final assertion:

```go
		assert.ErrorIs(t, err, ErrInvalidEvacuation)
```

(Leave the `"unknown from_host"` subtest asserting `ErrUnknownHost` — unchanged.)

- [ ] **Step 2: Add the API-level 400 test**

In `internal/api/evacuate_test.go`, add:

```go
func TestEvacuate_API_UnknownDestHost_400(t *testing.T) {
	srv, tok, mem := newEvacSrv(t)
	ctx := context.Background()
	seedEvac(t, mem, "db1")
	// db1 mapped to a host that does not exist -> bad map -> 400, no job.
	resp := postEvacuate(t, srv, tok, instance.EvacuateRequest{
		FromHost: "h1", Map: map[string]string{"db1": "ghost"},
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	all, err := mem.ListJobs(ctx, store.JobFilter{})
	require.NoError(t, err)
	assert.Empty(t, all, "no job should be enqueued when validation fails")
}
```

- [ ] **Step 3: Run both tests to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestResolveEvacuation/unknown_dest_host -v`
Expected: FAIL (still returns `ErrUnknownHost`).

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/ -run TestEvacuate_API_UnknownDestHost_400 -v`
Expected: FAIL (currently 404).

- [ ] **Step 4: Change the unknown-dest-host branch**

In `internal/instance/evacuate.go`, inside the `for slug, dest := range req.Map` loop, replace:

```go
		if _, ok := s.host(dest); !ok {
			return nil, ErrUnknownHost
		}
```

with:

```go
		if _, ok := s.host(dest); !ok {
			return nil, fmt.Errorf("%w: no such destination host %q for slug %q",
				ErrInvalidEvacuation, dest, slug)
		}
```

(`fmt` is already imported.) Update the `ErrInvalidEvacuation` doc comment to mention the unknown-destination case.

- [ ] **Step 5: Run both tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ ./internal/api/ -run 'TestResolveEvacuation|TestEvacuate_API' -v`
Expected: PASS.

- [ ] **Step 6: gofmt + vet + commit**

```bash
git add internal/instance/evacuate.go internal/instance/evacuate_test.go internal/api/evacuate_test.go
git commit -m "fix(evacuate): unknown destination host in map returns 400, not 404 (#54)"
```

---

### Task 3: evacuate concurrency — flag + per-request override

**Files:**
- Modify: `internal/instance/evacuate.go` (`EvacuateRequest` field)
- Modify: `internal/evacuate/handler.go` (remove var, add field + clamp logic)
- Modify: `internal/evacuate/handler_test.go` (stop mutating the removed var; add override/clamp tests)
- Modify: `cmd/podman-api/main.go` (flag + wiring)

- [ ] **Step 1: Add the request field**

In `internal/instance/evacuate.go`, add to `EvacuateRequest`:

```go
	// Concurrency, if >0, overrides the server's default for how many child
	// migrations run at once (clamped to [1,32] by the handler). Request-only:
	// it does not affect the migrate plan.
	Concurrency int `json:"concurrency,omitempty"`
```

- [ ] **Step 2: Rewrite `TestEvacuateConcurrencyBound` and add override/clamp tests**

In `internal/evacuate/handler_test.go`, replace the `old := evacuateConcurrency … defer` lines in `TestEvacuateConcurrencyBound` with setting the handler field. The test builds `h := &Handler{Svc: svc, Jobs: mem}` — set `h.Concurrency = 1` instead of the package var, and keep the rest:

```go
func TestEvacuateConcurrencyBound(t *testing.T) {
	ctx := context.Background()
	svc, mem := buildSvc(t, "a", "b", "c", "d")
	parent, _ := mem.Enqueue(ctx, "evacuate", mustArgs(t, "hostA",
		map[string]string{"a": "hostB", "b": "hostB", "c": "hostB", "d": "hostB"}), "")

	var inFlight, maxInFlight int32
	h := &Handler{Svc: svc, Jobs: mem, Concurrency: 1}
	h.migrate = func(_ context.Context, req instance.MigrateRequest, step func(string, string)) error {
		n := atomic.AddInt32(&inFlight, 1)
		for {
			m := atomic.LoadInt32(&maxInFlight)
			if n <= m || atomic.CompareAndSwapInt32(&maxInFlight, m, n) {
				break
			}
		}
		for i := 0; i < 100000; i++ {
		}
		atomic.AddInt32(&inFlight, -1)
		return nil
	}
	if err := h.Run(ctx, parent, jobs.NewJobContext(mem, parent.ID)); err != nil {
		t.Fatal(err)
	}
	if maxInFlight != 1 {
		t.Fatalf("Concurrency=1 but observed %d in flight", maxInFlight)
	}
}
```

Add a small unit test for the clamp helper (see Step 4 for its name/signature):

```go
func TestEffectiveConcurrency(t *testing.T) {
	cases := []struct{ handler, req, want int }{
		{0, 0, defaultEvacuateConcurrency}, // both unset -> default
		{5, 0, 5},                          // handler default
		{5, 3, 3},                          // request overrides handler
		{0, 1000, maxEvacuateConcurrency},  // request clamped to max
		{0, -4, defaultEvacuateConcurrency},// negative request ignored -> default
		{1, 0, 1},                          // handler of 1 honoured
	}
	for _, c := range cases {
		if got := effectiveConcurrency(c.handler, c.req); got != c.want {
			t.Errorf("effectiveConcurrency(%d,%d)=%d, want %d", c.handler, c.req, got, c.want)
		}
	}
}
```

`mustArgs` currently marshals `(host, map)`. It does not need a concurrency arg for these tests (the override is unit-tested via `effectiveConcurrency` directly). Leave `mustArgs` unchanged.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/evacuate/ -run 'TestEvacuateConcurrencyBound|TestEffectiveConcurrency' -v`
Expected: FAIL — `evacuateConcurrency` removed / `effectiveConcurrency` undefined / `Concurrency` field unknown.

- [ ] **Step 4: Implement the handler changes**

In `internal/evacuate/handler.go`:

Remove:

```go
// evacuateConcurrency bounds how many child migrations run at once. migrate is
// heavy (stop + volume cold-copy + apply + verify), so keep it low. var (not
// const) so same-package tests can change it.
var evacuateConcurrency = 2
```

Add (near the top, after imports):

```go
const (
	// defaultEvacuateConcurrency bounds how many child migrations run at once
	// when neither the handler nor the request specifies one. migrate is heavy
	// (stop + volume cold-copy + apply + verify), so keep it low.
	defaultEvacuateConcurrency = 2
	// maxEvacuateConcurrency caps any operator/client value so a fat-fingered
	// body can't spawn an unbounded number of migration goroutines.
	maxEvacuateConcurrency = 32
)

// effectiveConcurrency resolves the child-migration bound: a positive request
// value wins, else a positive handler default, else the package default; the
// result is clamped to [1, maxEvacuateConcurrency].
func effectiveConcurrency(handlerDefault, reqOverride int) int {
	n := defaultEvacuateConcurrency
	if handlerDefault > 0 {
		n = handlerDefault
	}
	if reqOverride > 0 {
		n = reqOverride
	}
	if n < 1 {
		n = 1
	}
	if n > maxEvacuateConcurrency {
		n = maxEvacuateConcurrency
	}
	return n
}
```

Add the field to `Handler`:

```go
	// Concurrency is the default child-migration bound (0 -> defaultEvacuateConcurrency).
	// A request's Concurrency overrides it per call. See effectiveConcurrency.
	Concurrency int
```

In `Run`, replace:

```go
	sem := make(chan struct{}, evacuateConcurrency)
```

with:

```go
	sem := make(chan struct{}, effectiveConcurrency(h.Concurrency, req.Concurrency))
```

Update the type-doc comment on `Handler` that currently says "with the default pool of 4, four concurrent evacuates…" only if it references the removed var; leave the broader note intact.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/evacuate/ -count=1 -v`
Expected: PASS (all evacuate tests).

- [ ] **Step 6: Wire the flag in `main.go`**

In `cmd/podman-api/main.go`, add to the `flag` var block (after `jobsRetention`):

```go
		evacuateConcurrency = flag.Int("evacuate-concurrency", 2, "max child migrations an evacuate runs at once (1..32); a request's \"concurrency\" overrides per call")
```

Update the evacuate handler construction (currently `"evacuate": &evacuate.Handler{Svc: svc, Jobs: db},`) to:

```go
			"evacuate": &evacuate.Handler{Svc: svc, Jobs: db, Concurrency: *evacuateConcurrency},
```

- [ ] **Step 7: Build, gofmt, vet**

Run: `make build`
Expected: builds `bin/podman-api` with no error.

Run: `gofmt -l cmd/ internal/ ; go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./...`
Expected: no output / clean.

- [ ] **Step 8: Commit**

```bash
git add internal/instance/evacuate.go internal/evacuate/handler.go internal/evacuate/handler_test.go cmd/podman-api/main.go
git commit -m "feat(evacuate): -evacuate-concurrency flag + per-request concurrency override (#54)"
```

---

### Task 4: docs — OpenAPI + README + wiki

**Files:**
- Modify: `api/openapi.yaml` (evacuate request body: `concurrency`)
- Modify: `README.md` (the `-evacuate-concurrency` flag bullet)
- Wiki (separate repo, push directly): `Operating.md` and/or `Deploying.md`

- [ ] **Step 1: OpenAPI — add `concurrency` to the evacuate request schema**

In `api/openapi.yaml`, find the `/evacuate` POST request body schema (the one with `from_host` and `map`) and add an optional integer property:

```yaml
                concurrency:
                  type: integer
                  minimum: 1
                  maximum: 32
                  description: >-
                    Optional: max child migrations to run at once for this
                    evacuate. Overrides the server's -evacuate-concurrency
                    default; clamped to [1,32].
```

(Match the surrounding indentation exactly — verify by reading the existing `from_host`/`map` properties first.)

- [ ] **Step 2: README — document the flag**

In `README.md`, near the other daemon flags (where `-jobs-retention` is listed), add a bullet:

```
- `-evacuate-concurrency <n>` — max child migrations an evacuate runs at once (default 2, range 1..32). A request body's `"concurrency"` overrides it per call.
```

- [ ] **Step 3: Commit repo docs**

```bash
git add api/openapi.yaml README.md
git commit -m "docs: evacuate concurrency flag + per-request override (#54)"
```

- [ ] **Step 4: Wiki (post-merge, separate repo)**

After the PR merges, update the wiki `Operating.md` "Migrating & evacuating" / "Clear a whole host" section to mention tuning concurrency (flag default + per-request `concurrency`), and push directly (`/tmp/pa-wiki`). The wiki has no PR flow. (This is a post-merge step, not part of the PR.)

---

## Final verification (before finishing the branch)

- [ ] `make test` — full suite green.
- [ ] `make build` — binary builds.
- [ ] `gofmt -l .` empty; `go vet` clean (with tags).
- [ ] `go test -tags "..." -race ./internal/store/ -run TestSQLite_ClaimNext_NoDoubleClaim -count=20` — 20/20.
- [ ] Dispatch a final reviewer subagent over the whole branch diff.
- [ ] Use superpowers:finishing-a-development-branch → push + open PR closing the three #54 sub-items.
