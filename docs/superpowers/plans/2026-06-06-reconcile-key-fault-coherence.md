# #113 Coherent recoverable key-fault handling — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the GCM-decrypt failure out of `ErrSpecCorrupt` into a new
recoverable `ErrSecretsUndecryptable` sentinel, so reconcile treats both
key-file misconfigurations (no key / wrong key) as a single visible-but-retrying
state, while genuine plaintext-JSON corruption stays terminal.

**Architecture:** A new store sentinel returned only from `GetSpec`'s `open()`
failure path. Reconcile's `destSpecState` gains a `specNeedsKey` state covering
`ErrSecretsNeedKey` + `ErrSecretsUndecryptable` → inconclusive retry with a distinct
`reconcile-needs-key` step. UI/API error mappers learn the new sentinel (mapped
identically to the old `ErrSpecCorrupt` 422/degrade behavior) so the split is
behavior-preserving for synchronous callers.

**Tech Stack:** Go 1.22, SQLite, AES-256-GCM. Build/test with the remote-client tags
(`make test`, or `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs
exclude_graphdriver_devicemapper" ./internal/<pkg>/ -run <Pat> -v`).

Build tags are mandatory — bare `go test ./...` fails to compile (CGO graphdrivers).

---

### Task 1: Store — `ErrSecretsUndecryptable` sentinel + `GetSpec` decrypt path

**Files:**
- Modify: `internal/store/store.go` (add sentinel)
- Modify: `internal/store/sqlite.go` (`GetSpec` decrypt-fail return)
- Test: `internal/store/sqlite_test.go`

- [ ] **Step 1: Update the failing test (wrong-key now → new sentinel)**

In `internal/store/sqlite_test.go`, rename `TestSQLite_GetSpec_WrongKey_IsErrSpecCorrupt`
to `TestSQLite_GetSpec_WrongKey_IsErrSecretsUndecryptable` and change its assertion:

```go
func TestSQLite_GetSpec_WrongKey_IsErrSecretsUndecryptable(t *testing.T) {
	ctx := context.Background()
	ks := NewKeyStore(testKey(0x11))
	s := openTestStore(t, ks)
	require.NoError(t, s.PutSpec(ctx, sampleSpec()))
	ks.Store(testKey(0x22)) // rotate to the wrong key → decrypt fails
	_, err := s.GetSpec(ctx, "h1", "postgres", "demo")
	// A wrong/missing key is recoverable by a restart with the correct key — it is
	// NOT the permanent-corruption ErrSpecCorrupt.
	require.ErrorIs(t, err, ErrSecretsUndecryptable)
	require.NotErrorIs(t, err, ErrSpecCorrupt)
}
```

Leave `TestSQLite_GetSpec_CorruptSecretsJSON_IsErrSpecCorrupt` (post-decrypt
unmarshal) asserting `ErrSpecCorrupt` — it must stay terminal.

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/ -run TestSQLite_GetSpec_WrongKey -v`
Expected: FAIL — `ErrSecretsUndecryptable` undefined (compile error), then assertion.

- [ ] **Step 3: Add the sentinel**

In `internal/store/store.go`, after `ErrSpecCorrupt`:

```go
// ErrSecretsUndecryptable marks a spec whose sealed secrets blob will not open
// under the loaded key: the daemon was started with the WRONG -spec-key-file
// (or, rarer, the ciphertext is corrupt — the two are indistinguishable at the
// GCM layer). Unlike ErrSpecCorrupt (permanently malformed plaintext) this is
// recoverable: a restart with the correct key file makes the row readable again,
// so callers (boot reconciliation) keep retrying rather than failing terminally.
var ErrSecretsUndecryptable = errors.New("store: spec secrets undecryptable (wrong or missing -spec-key-file)")
```

- [ ] **Step 4: Change the `GetSpec` decrypt-fail return**

In `internal/store/sqlite.go` `GetSpec`, the `open()` failure branch only:

```go
secJSON, err := open(s.keys.Load(), blob)
if err != nil {
	return Spec{}, fmt.Errorf("%w: decrypt secrets: %v", ErrSecretsUndecryptable, err)
}
```

Leave the params/secrets-post-decrypt/domains unmarshal returns as `ErrSpecCorrupt`.
Update `ErrSpecCorrupt`'s doc comment so it no longer claims to cover "no longer
decrypts" (that case is now `ErrSecretsUndecryptable`); it covers malformed JSON
columns and post-decrypt malformed plaintext only.

- [ ] **Step 5: Run the store tests**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/ -run TestSQLite_GetSpec -v`
Expected: PASS (wrong-key → undecryptable; the three JSON-corruption tests still → ErrSpecCorrupt).

- [ ] **Step 6: gofmt + commit**

```bash
gofmt -w internal/store/store.go internal/store/sqlite.go internal/store/sqlite_test.go
git add internal/store/
git commit -m "store: split decrypt failure into ErrSecretsUndecryptable (#113)"
```

---

### Task 2: Reconcile — `specNeedsKey` state + `reconcile-needs-key` step

**Files:**
- Modify: `internal/instance/reconcile.go` (`specState`, `destSpecState`, caller)
- Test: `internal/instance/reconcile_test.go`

- [ ] **Step 1: Write the failing test**

In `internal/instance/reconcile_test.go`, add after `TestReconcileMigrate_DestSpecCorrupt_Terminal`:

```go
// TestReconcileMigrate_DestSpecNeedsKey_Inconclusive verifies a key-file
// misconfiguration on the dest spec (no key OR wrong key) is recoverable: the
// reconcile stays inconclusive (retries) and mutates neither host, so a restart
// with the correct -spec-key-file resumes the in-flight migrate. Contrast with
// _DestSpecCorrupt_Terminal: genuine JSON corruption is terminal.
func TestReconcileMigrate_DestSpecNeedsKey_Inconclusive(t *testing.T) {
	for name, keyErr := range map[string]error{
		"no key":    store.ErrSecretsNeedKey,
		"wrong key": store.ErrSecretsUndecryptable,
	} {
		t.Run(name, func(t *testing.T) {
			svc, fc, st := reconcileSvc(t)
			svc.SetStore(&getSpecErrStore{Memory: st, err: keyErr})
			fc.AddPod("h1", healthyPod("web-x")) // source present
			fc.AddPod("h2", healthyPod("web-x")) // dest healthy

			var steps []string
			resolved, ok, _, err := svc.ReconcileMigrate(context.Background(), req(),
				func(s, _ string) { steps = append(steps, s) })
			if err != nil {
				t.Fatal(err)
			}
			if resolved || ok {
				t.Fatalf("got resolved=%v ok=%v, want false/false (recoverable: inconclusive retry)", resolved, ok)
			}
			if !slices.Contains(steps, "reconcile-needs-key") {
				t.Fatalf("steps = %v, want a visible reconcile-needs-key step", steps)
			}
			// Neither host may be mutated on a recoverable inconclusive result.
			if _, err := fc.PodInspect(context.Background(), "h2", "web-x"); err != nil {
				t.Fatalf("dest deleted on a recoverable key fault (data loss): %v", err)
			}
			if _, err := fc.PodInspect(context.Background(), "h1", "web-x"); err != nil {
				t.Fatalf("source mutated on a recoverable key fault: %v", err)
			}
		})
	}
}
```

Add `"slices"` to the test file's imports if not already present.

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestReconcileMigrate_DestSpecNeedsKey -v`
Expected: FAIL — no `reconcile-needs-key` step (the key errors currently hit the `default → specInconclusive` / `specCorrupt` arms).

- [ ] **Step 3: Add `specNeedsKey` and classify it**

In `internal/instance/reconcile.go`, add to the `specState` const block (after `specCorrupt`):

```go
	specNeedsKey                      // key missing/wrong (ErrSecretsNeedKey / ErrSecretsUndecryptable): recoverable, retry visibly
```

In `destSpecState`, add a case before `specCorrupt`:

```go
	case errors.Is(err, store.ErrSecretsNeedKey), errors.Is(err, store.ErrSecretsUndecryptable):
		return specNeedsKey
```

Update the `destSpecState` doc comment to describe `specNeedsKey`.

- [ ] **Step 4: Handle `specNeedsKey` in the caller**

In `ReconcileMigrate`, in the `if ds == destHealthy {` block, add after the
`specInconclusive` check (and before `specCorrupt`):

```go
		if spec == specNeedsKey {
			// The dest spec's secrets can't be read because the daemon's key file
			// is missing or wrong — a static config fault recoverable by restarting
			// with the correct -spec-key-file. Stay inconclusive (retry) and mutate
			// nothing: a future reconcile reads the spec once the key is fixed,
			// without re-issuing the migrate. Distinct, visible step so the operator
			// sees the *configuration* is the problem.
			step("reconcile-needs-key", "destination spec secrets unreadable (key missing or wrong); restart with -spec-key-file")
			return false, false, "", nil
		}
```

- [ ] **Step 5: Run the reconcile tests**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestReconcileMigrate -v`
Expected: PASS — new test green; `_DestSpecCorrupt_Terminal` and `_DestSpecLookupError_Inconclusive` still green.

- [ ] **Step 6: gofmt + commit**

```bash
gofmt -w internal/instance/reconcile.go internal/instance/reconcile_test.go
git add internal/instance/
git commit -m "reconcile: recoverable specNeedsKey state for key-file faults (#113)"
```

---

### Task 3: UI + API ripple — map `ErrSecretsUndecryptable` like `ErrSpecCorrupt`

**Files:**
- Modify: `internal/ui/render.go` (`errorStatus`)
- Modify: `internal/ui/handlers_secrets.go` (`secretsFormData`)
- Modify: `internal/api/errors.go` (`classify`)
- Test: `internal/ui/render_test.go`, `internal/ui/handlers_secrets_test.go`, `internal/api/coverage_test.go`

- [ ] **Step 1: Write the failing tests**

In `internal/ui/render_test.go` `TestErrorStatus` cases map, add:

```go
		store.ErrSecretsUndecryptable: http.StatusUnprocessableEntity,
```

In `internal/api/coverage_test.go` `TestClassify_RemainingSentinels` cases slice, add:

```go
		{store.ErrSecretsUndecryptable, "secrets_undecryptable", http.StatusUnprocessableEntity},
```

In `internal/ui/handlers_secrets_test.go`, add a sibling degrade test + store:

```go
func TestSecretsFormUndecryptableSpecDegrades(t *testing.T) {
	u, mem := uiWithSecretInstance(t)
	u.cfg.Svc.SetStore(undecryptableSpecStore{mem})
	body := authedGet(t, u, "/ui/hosts/edge-1/instances/demo/main/secrets").Body.String()
	if !strings.Contains(body, "manual cleanup") {
		t.Error("an undecryptable spec should degrade to a cleanup notice")
	}
	if strings.Contains(body, `type="password"`) {
		t.Error("no rotate inputs should render for an undecryptable spec")
	}
}

// undecryptableSpecStore makes GetSpec report a wrong/missing-key blob.
type undecryptableSpecStore struct{ *store.Memory }

func (undecryptableSpecStore) GetSpec(context.Context, string, string, string) (store.Spec, error) {
	return store.Spec{}, store.ErrSecretsUndecryptable
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/ ./internal/api/ -run "TestErrorStatus|TestClassify_RemainingSentinels|TestSecretsFormUndecryptableSpecDegrades" -v`
Expected: FAIL — undecryptable currently maps to 500 / does not degrade.

- [ ] **Step 3: Map the sentinel in `errorStatus`**

In `internal/ui/render.go`, extend the 422 case:

```go
	case errors.Is(err, instance.ErrHostSecretMissing),
		errors.Is(err, store.ErrSpecCorrupt),
		errors.Is(err, store.ErrSecretsUndecryptable):
		return http.StatusUnprocessableEntity
```

- [ ] **Step 4: Map the sentinel in `classify`**

In `internal/api/errors.go`, add before the `ErrSpecCorrupt` case:

```go
	case errors.Is(err, store.ErrSecretsUndecryptable):
		return "secrets_undecryptable", http.StatusUnprocessableEntity, err.Error()
```

- [ ] **Step 5: Degrade in `secretsFormData`**

In `internal/ui/handlers_secrets.go`, change the corrupt branch:

```go
		if errors.Is(err, store.ErrSpecCorrupt) || errors.Is(err, store.ErrSecretsUndecryptable) {
			data["Corrupt"] = true
```

(keep the rest of that branch — it returns nil err so the page degrades.)

- [ ] **Step 6: Run the tests**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/ ./internal/api/ -v 2>&1 | tail -20`
Expected: PASS (new cases green, existing green).

- [ ] **Step 7: gofmt + commit**

```bash
gofmt -w internal/ui/render.go internal/ui/handlers_secrets.go internal/api/errors.go internal/ui/render_test.go internal/ui/handlers_secrets_test.go internal/api/coverage_test.go
git add internal/ui/ internal/api/
git commit -m "ui,api: map ErrSecretsUndecryptable to 422/degrade like ErrSpecCorrupt (#113)"
```

---

### Final verification (after all tasks)

- [ ] `gofmt -l internal/` is empty.
- [ ] `make build` succeeds.
- [ ] `make test` passes (full unit suite).
