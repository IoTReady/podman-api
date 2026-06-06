# Store/Reconcile Consistency Follow-ups (#117) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `GetHostSecret`'s wrong-key error coherent with the spec path (typed `ErrSecretsUndecryptable` → 422), and bound reconcile step growth by coalescing consecutive identical job steps in the store layer.

**Architecture:** Two independent changes. (1) Wrap `GetHostSecret`'s `open()` failure in the existing `ErrSecretsUndecryptable` sentinel — no new type, no caller control-flow change. (2) Add a `Count` field to `store.JobStep` and coalesce consecutive identical steps inside both `AppendStep` implementations (sqlite + memory), surfacing the count in the jobs API view and the job-detail UI template.

**Tech Stack:** Go 1.22, SQLite state store, AES-256-GCM sealed secrets, html/template, testify.

**Build/test (MANDATORY tags — bare `go test ./...` fails to compile):**
```sh
go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/<pkg>/ -run <Pat> -v
```
or `make test`. Keep `gofmt -l .` empty (do NOT gofmt `.html`) and `go vet` clean.

---

### Task 1: `GetHostSecret` returns `ErrSecretsUndecryptable` on wrong-key

**Files:**
- Modify: `internal/store/sqlite.go:437-451` (`GetHostSecret`)
- Test: `internal/store/host_secrets_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/store/host_secrets_test.go` (it already has `forEachStore`-style
helpers / `openHostSecretStore`; mirror the file's existing pattern — seal under
one key, rotate the keystore to a wrong key, read back). If the file's helper
opens with a fixed `KeyStore`, use a locally-constructed `*KeyStore` so the test
can rotate it:

```go
func TestHostSecret_WrongKey_IsErrSecretsUndecryptable(t *testing.T) {
	ctx := context.Background()
	keys := &KeyStore{}
	keys.Store([32]byte{1, 2, 3})
	sq, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), keys)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	if err := sq.PutHostSecret(ctx, "h1", "shared-token", []byte("v")); err != nil {
		t.Fatalf("PutHostSecret: %v", err)
	}
	keys.Store([32]byte{9, 9, 9}) // rotate to the wrong key → decrypt fails
	_, err = sq.GetHostSecret(ctx, "h1", "shared-token")
	if !errors.Is(err, ErrSecretsUndecryptable) {
		t.Fatalf("want ErrSecretsUndecryptable, got %v", err)
	}
}
```

Confirm the exact `PutHostSecret` signature and the store-open helper already in
`host_secrets_test.go` and match them (the round-trip test at the top of that
file is the reference). Ensure `errors`, `filepath` imports are present.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/ -run TestHostSecret_WrongKey -v`
Expected: FAIL — current code returns the raw GCM error, not `ErrSecretsUndecryptable`.

- [ ] **Step 3: Wrap the `open()` failure**

In `internal/store/sqlite.go`, change the tail of `GetHostSecret`:

```go
	val, err := open(s.keys.Load(), blob)
	if err != nil {
		// A wrong/missing key is recoverable by a restart with the correct
		// -spec-key-file; surface the typed sentinel so the API/UI classify it
		// 422 (coherent with GetSpec), not a raw 500. (#117)
		return nil, fmt.Errorf("%w: decrypt host secret: %v", ErrSecretsUndecryptable, err)
	}
	return val, nil
```

(Replaces the bare `return open(s.keys.Load(), blob)`.) Confirm `fmt` is already
imported in `sqlite.go` (it is).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/ -run TestHostSecret -v`
Expected: PASS (all `TestHostSecret*`).

- [ ] **Step 5: Verify no caller regressed**

Run the migrate/instance suite (the only `GetHostSecret` callers live there):
`go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -v`
Expected: PASS. Both callers (`migrate.go:119`, `:293`) branch only on
`ErrNotFound` and treat any other error generically, so the new wrap is
transparent to them.

- [ ] **Step 6: Commit**

```bash
git add internal/store/sqlite.go internal/store/host_secrets_test.go
git commit -m "fix(store): GetHostSecret wraps wrong-key decrypt in ErrSecretsUndecryptable (#117)"
```

---

### Task 2: Coalesce consecutive identical job steps

**Files:**
- Modify: `internal/store/jobs.go` (`JobStep` struct — add `Count`)
- Modify: `internal/store/sqlite.go:635-658` (`AppendStep`)
- Modify: `internal/store/memory.go:208-219` (`AppendStep`)
- Modify: `internal/api/jobs.go` (`stepView` + `toJobView` loop)
- Modify: `internal/ui/templates/job-detail.html`
- Test: `internal/store/jobs_test.go`, `internal/store/memory_jobs_test.go`

- [ ] **Step 1: Add `Count` to `JobStep`**

In `internal/store/jobs.go`, extend the struct (keep field tags aligned):

```go
type JobStep struct {
	TS     time.Time `json:"ts"`
	Step   string    `json:"step"`
	Detail string    `json:"detail,omitempty"`
	// Count is the total number of consecutive identical occurrences of this
	// step, materialized only when coalesced (>1). 0/omitted ⇒ a single
	// occurrence. AppendStep collapses consecutive identical (Step, Detail)
	// rows so a long-looping reconcile can't grow the array unboundedly. (#117)
	Count int `json:"count,omitempty"`
}
```

- [ ] **Step 2: Write failing store tests**

Add to `internal/store/jobs_test.go` (sqlite — uses `openJobStore(t)`):

```go
func TestSQLite_AppendStep_CoalescesConsecutiveIdentical(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)
	j, _ := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	_, _, _ = s.ClaimNext(ctx)
	mustStep := func(ts int64, step, detail string) {
		if err := s.AppendStep(ctx, j.ID, JobStep{TS: time.Unix(ts, 0), Step: step, Detail: detail}); err != nil {
			t.Fatalf("AppendStep: %v", err)
		}
	}
	mustStep(100, "reconcile-needs-key", "restart with -spec-key-file")
	mustStep(160, "reconcile-needs-key", "restart with -spec-key-file") // identical → coalesce
	got, _ := s.GetJob(ctx, j.ID)
	if len(got.Steps) != 1 {
		t.Fatalf("want 1 coalesced step, got %d: %+v", len(got.Steps), got.Steps)
	}
	if got.Steps[0].Count != 2 {
		t.Fatalf("want Count=2, got %d", got.Steps[0].Count)
	}
	if !got.Steps[0].TS.Equal(time.Unix(160, 0)) {
		t.Fatalf("want latest TS refreshed to 160, got %v", got.Steps[0].TS)
	}
}

func TestSQLite_AppendStep_DistinctStepsDoNotCoalesce(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)
	j, _ := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	_, _, _ = s.ClaimNext(ctx)
	must := func(step, detail string) {
		if err := s.AppendStep(ctx, j.ID, JobStep{TS: time.Unix(1, 0), Step: step, Detail: detail}); err != nil {
			t.Fatalf("AppendStep: %v", err)
		}
	}
	must("reconcile-needs-key", "x")     // 1
	must("reconcile-inconclusive", "h1") // 2 — different step
	must("reconcile-needs-key", "x")     // 3 — identical to #1 but NOT consecutive
	got, _ := s.GetJob(ctx, j.ID)
	if len(got.Steps) != 3 {
		t.Fatalf("non-consecutive identicals must stay separate; want 3, got %d: %+v", len(got.Steps), got.Steps)
	}
	if got.Steps[0].Count != 0 {
		t.Fatalf("single occurrence must leave Count=0, got %d", got.Steps[0].Count)
	}
}
```

Add the analogous pair to `internal/store/memory_jobs_test.go` against `NewMemory()`
(it uses `m.Enqueue`/`m.AppendStep`/`m.GetJob`; mirror the existing tests in that
file — no `ClaimNext` needed if the existing memory tests don't claim, but match
whatever the neighbouring `TestMemory_Jobs_*` tests do):

```go
func TestMemory_AppendStep_CoalescesConsecutiveIdentical(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	j, _ := m.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	step := JobStep{TS: time.Unix(100, 0), Step: "reconcile-needs-key", Detail: "d"}
	if err := m.AppendStep(ctx, j.ID, step); err != nil {
		t.Fatalf("AppendStep: %v", err)
	}
	step.TS = time.Unix(160, 0)
	if err := m.AppendStep(ctx, j.ID, step); err != nil {
		t.Fatalf("AppendStep: %v", err)
	}
	got, _ := m.GetJob(ctx, j.ID)
	if len(got.Steps) != 1 || got.Steps[0].Count != 2 || !got.Steps[0].TS.Equal(time.Unix(160, 0)) {
		t.Fatalf("want 1 step Count=2 TS=160, got %+v", got.Steps)
	}
}

func TestMemory_AppendStep_DistinctStepsDoNotCoalesce(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	j, _ := m.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	for _, s := range []JobStep{
		{Step: "a", Detail: "x"}, {Step: "b", Detail: "y"}, {Step: "a", Detail: "x"},
	} {
		if err := m.AppendStep(ctx, j.ID, s); err != nil {
			t.Fatalf("AppendStep: %v", err)
		}
	}
	got, _ := m.GetJob(ctx, j.ID)
	if len(got.Steps) != 3 {
		t.Fatalf("want 3, got %d: %+v", len(got.Steps), got.Steps)
	}
}
```

Confirm `time`/`encoding/json` imports exist in each test file (sqlite test file
already imports both; add to the memory test file if missing).

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/ -run 'AppendStep_Coalesce|AppendStep_Distinct' -v`
Expected: FAIL — current `AppendStep` always appends (the coalesce assertions and `Count` are unmet). The non-consecutive test may already pass; the coalesce tests must fail.

- [ ] **Step 4: Implement coalesce in sqlite `AppendStep`**

In `internal/store/sqlite.go`, replace the `arr = append(arr, step)` line in
`AppendStep` with:

```go
	if n := len(arr); n > 0 && arr[n-1].Step == step.Step && arr[n-1].Detail == step.Detail {
		// Collapse a consecutive identical step (e.g. a reconcile loop stuck on
		// the same condition): bump the occurrence count and refresh the
		// timestamp to the latest attempt rather than growing the array. (#117)
		if arr[n-1].Count == 0 {
			arr[n-1].Count = 1
		}
		arr[n-1].Count++
		arr[n-1].TS = step.TS
	} else {
		arr = append(arr, step)
	}
```

- [ ] **Step 5: Implement coalesce in memory `AppendStep`**

In `internal/store/memory.go` `AppendStep`, replace the
`m.jobs[i].Steps = append(m.jobs[i].Steps, step)` line with the same coalesce
logic operating on `m.jobs[i].Steps`:

```go
			steps := m.jobs[i].Steps
			if n := len(steps); n > 0 && steps[n-1].Step == step.Step && steps[n-1].Detail == step.Detail {
				if steps[n-1].Count == 0 {
					steps[n-1].Count = 1
				}
				steps[n-1].Count++
				steps[n-1].TS = step.TS
			} else {
				m.jobs[i].Steps = append(steps, step)
			}
			return nil
```

- [ ] **Step 6: Run store tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/ -v`
Expected: PASS (new tests + all existing job-step tests, e.g.
`TestSQLite_AppendStep_Finish_FailRunning` which appends a single step and still
expects `len==1`, `Count==0`).

- [ ] **Step 7: Surface `Count` in the jobs API view**

In `internal/api/jobs.go`: add `Count int json:"count,omitempty"` to the
`stepView` struct (next to `Detail`), and populate it in the `toJobView` loop:

```go
	for _, s := range j.Steps {
		v.Steps = append(v.Steps, stepView{
			TS: s.TS.UTC().Format(time.RFC3339), Step: s.Step, Detail: s.Detail, Count: s.Count,
		})
	}
```

- [ ] **Step 8: Surface the count in the job-detail UI**

In `internal/ui/templates/job-detail.html`, change the steps list item to render
the count when present (do NOT gofmt this file):

```html
<ol>{{range .Job.Steps}}<li>{{.Step}} {{.Detail}}{{if .Count}} (×{{.Count}}){{end}}</li>{{end}}</ol>
```

- [ ] **Step 9: Run the api + ui suites**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/ ./internal/ui/ -v`
Expected: PASS. If a jobs-API golden/JSON test asserts an exact step object
shape, update it to tolerate the new optional `count` field (omitted when 0).

- [ ] **Step 10: Full suite + lint**

Run: `make test` (full suite), then `gofmt -l .` (must be empty — `.html` is not
Go, leave it) and `go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./...`.
Expected: all green, `gofmt -l` empty, vet clean.

- [ ] **Step 11: Commit**

```bash
git add internal/store/jobs.go internal/store/sqlite.go internal/store/memory.go \
        internal/store/jobs_test.go internal/store/memory_jobs_test.go \
        internal/api/jobs.go internal/ui/templates/job-detail.html
git commit -m "feat(jobs): coalesce consecutive identical steps to bound reconcile loops (#117)"
```

---

## Self-review notes

- Spec coverage: Task 1 ⇒ spec §1; Task 2 ⇒ spec §2 (schema, both stores, API,
  UI, all three test cases). No spec requirement left unimplemented.
- Type consistency: `Count int` is named identically across `JobStep`,
  `stepView`, and both `AppendStep` bodies; `ErrSecretsUndecryptable` is the
  existing sentinel (no new type).
- No placeholders: every code step shows the exact code; test bodies are
  complete. The two "confirm the neighbouring pattern" notes are verification
  steps against real existing helpers, not deferred work.
