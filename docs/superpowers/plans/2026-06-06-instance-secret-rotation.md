# Per-instance Secret Rotation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an Admin UI to view and rotate an instance's per-instance secrets, and make a permanently-undecryptable spec a terminal reconcile failure instead of an infinite retry.

**Architecture:** A typed `store.ErrSpecCorrupt` distinguishes permanent decode/decrypt failures in `GetSpec` from transient store errors; `reconcile.destSpecState` maps it to a terminal `failed`. A `Service.RotateInstanceSecrets` mirrors `UpgradeImage` (load spec → overlay secrets → `Apply(Replace=true)`), and a write-only `Service.InstanceSecretState` reports per-name presence. A new UI page (GET form + POST rotate, following the upgrade-form pattern) drives it; secrets ride the POST body only (#99).

**Tech Stack:** Go 1.22 (`net/http.ServeMux`), `html/template` + `embed.FS`, HTMX, modernc SQLite, AES-256-GCM sealed secrets.

**Build/test (ALWAYS use the Makefile tags — bare `go test ./...` fails on the CGO graphdriver imports):**
- Whole worktree: `make -C /home/tej/projects/podman-api/.worktrees/feat-92-secret-rotation test`
- Single package, e.g.: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run <Pattern> ./internal/store/ -v` (run from the worktree root)
- `gofmt -l internal/...` must be empty for `.go` files (do NOT gofmt `.html`); `go vet` with the tags must be clean.

**Worktree:** Already created at `.worktrees/feat-92-secret-rotation` on branch `feat/92-secret-rotation`. Work there.

---

## File Structure

- `internal/store/store.go` — add `ErrSpecCorrupt` sentinel (Task 1).
- `internal/store/sqlite.go` — wrap `GetSpec`'s four decode/decrypt points (Task 1).
- `internal/store/sqlite_test.go` — corruption-classification tests (Task 1).
- `internal/instance/reconcile.go` — `specState` enum, `destSpecState` rewrite, caller switch, doc fix (Task 2).
- `internal/instance/reconcile_test.go` — corrupt→terminal test; correct stale comment (Task 2).
- `internal/instance/service.go` — `RotateInstanceSecrets`, `InstanceSecretState` (Task 3).
- `internal/instance/service_test.go` — rotation + presence tests (Task 3).
- `internal/ui/handlers_instances.go` — `instanceView` gains `HasSecrets`; new `templatePerInstanceSecrets` helper; thread `ctx` (Task 4).
- `internal/ui/handlers_deploy.go` — thread `ctx` into the two `instanceView` calls (Task 4).
- `internal/ui/templates/instance-detail.html` — Manage-secrets button + Notice line (Task 4).
- `internal/ui/handlers_secrets.go` — new: `secretsForm`, `secretsRotate`, `secretsFormData` (Task 5).
- `internal/ui/ui.go` — register the two `/secrets` routes (Task 5).
- `internal/ui/templates/secrets-form.html` — new template (Task 5).
- `internal/ui/handlers_secrets_test.go` — new: UI tests (Task 5).

---

## Task 1: Typed `ErrSpecCorrupt` + `GetSpec` wrapping

**Files:**
- Modify: `internal/store/store.go` (add sentinel after `ErrSecretsNeedKey`, ~line 17)
- Modify: `internal/store/sqlite.go` (`GetSpec`, lines 352–372)
- Test: `internal/store/sqlite_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `internal/store/sqlite_test.go` (package `store`; `s.db` is accessible in-package; `require` already imported):

```go
func TestSQLite_GetSpec_WrongKey_IsErrSpecCorrupt(t *testing.T) {
	ctx := context.Background()
	ks := NewKeyStore(testKey(0x11))
	s := openTestStore(t, ks)
	require.NoError(t, s.PutSpec(ctx, sampleSpec()))
	ks.Store(testKey(0x22)) // rotate to the wrong key → decrypt fails
	_, err := s.GetSpec(ctx, "h1", "postgres", "demo")
	require.ErrorIs(t, err, ErrSpecCorrupt)
}

func TestSQLite_GetSpec_CorruptParamsJSON_IsErrSpecCorrupt(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	require.NoError(t, s.PutSpec(ctx, sampleSpec()))
	_, err := s.db.ExecContext(ctx,
		`UPDATE specs SET parameters='{not json' WHERE host='h1' AND template='postgres' AND slug='demo'`)
	require.NoError(t, err)
	_, err = s.GetSpec(ctx, "h1", "postgres", "demo")
	require.ErrorIs(t, err, ErrSpecCorrupt)
}

func TestSQLite_GetSpec_CorruptDomainsJSON_IsErrSpecCorrupt(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	require.NoError(t, s.PutSpec(ctx, sampleSpec()))
	_, err := s.db.ExecContext(ctx,
		`UPDATE specs SET domains='{not json' WHERE host='h1' AND template='postgres' AND slug='demo'`)
	require.NoError(t, err)
	_, err = s.GetSpec(ctx, "h1", "postgres", "demo")
	require.ErrorIs(t, err, ErrSpecCorrupt)
}

func TestSQLite_GetSpec_NotFound_IsNotErrSpecCorrupt(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	_, err := s.GetSpec(context.Background(), "h1", "x", "y")
	require.ErrorIs(t, err, ErrNotFound)
	require.NotErrorIs(t, err, ErrSpecCorrupt)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'TestSQLite_GetSpec_(WrongKey|CorruptParamsJSON|CorruptDomainsJSON|NotFound)' ./internal/store/ -v`
Expected: FAIL — `undefined: ErrSpecCorrupt` (compile error).

- [ ] **Step 3: Add the sentinel**

In `internal/store/store.go`, immediately after the `ErrSecretsNeedKey` declaration (line 17):

```go
// ErrSpecCorrupt marks a permanently unreadable spec row: the secrets blob no
// longer decrypts (key loss/rotation) or a JSON column is malformed. It is
// distinct from transient store errors (context cancellation, SQLITE_BUSY) and
// from the definitive ErrNotFound, so callers (e.g. boot reconciliation) can
// stop retrying a row that will never become readable.
var ErrSpecCorrupt = errors.New("store: spec row corrupt or undecryptable")
```

- [ ] **Step 4: Wrap GetSpec's decode/decrypt points**

In `internal/store/sqlite.go` `GetSpec`, replace the four permanent-failure returns (the `params` unmarshal, the `open()` decrypt, the `secrets` unmarshal, and the `domains` unmarshal) so each wraps `ErrSpecCorrupt`. Leave the `row.Scan`/`ErrNotFound`/`ErrSecretsNeedKey` returns untouched:

```go
	var params map[string]any
	if err := json.Unmarshal([]byte(paramsJSON), &params); err != nil {
		return Spec{}, fmt.Errorf("%w: parameters: %v", ErrSpecCorrupt, err)
	}
	var secrets map[string]string
	if len(blob) > 0 {
		if s.keys == nil {
			return Spec{}, ErrSecretsNeedKey
		}
		secJSON, err := open(s.keys.Load(), blob)
		if err != nil {
			return Spec{}, fmt.Errorf("%w: decrypt secrets: %v", ErrSpecCorrupt, err)
		}
		if err := json.Unmarshal(secJSON, &secrets); err != nil {
			return Spec{}, fmt.Errorf("%w: secrets: %v", ErrSpecCorrupt, err)
		}
	}
	var domains []string
	if err := json.Unmarshal([]byte(domainsJSON), &domains); err != nil {
		return Spec{}, fmt.Errorf("%w: domains: %v", ErrSpecCorrupt, err)
	}
```

(`fmt` is already imported in `sqlite.go`.)

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/ -v`
Expected: PASS (new tests pass; `TestSQLite_WrongKey_FailsDecrypt` and the rest still pass).

- [ ] **Step 6: Commit**

```bash
git add internal/store/store.go internal/store/sqlite.go internal/store/sqlite_test.go
git commit -m "feat(store): typed ErrSpecCorrupt for permanent GetSpec decode/decrypt failures (#92)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Reconcile classifies a corrupt dest spec as terminal

**Files:**
- Modify: `internal/instance/reconcile.go` (`destSpecState` lines 187–212; its caller lines 100–107 and the `if persisted {` at line 108)
- Test: `internal/instance/reconcile_test.go` (new test; correct the stale comment on the existing inconclusive test)

- [ ] **Step 1: Write the failing test**

Add to `internal/instance/reconcile_test.go` (reuses the existing `getSpecErrStore` double at line 300 and `reconcileSvc`/`req`/`healthyPod` helpers; `errors` and `store` already imported):

```go
// TestReconcileMigrate_DestSpecCorrupt_Terminal verifies a permanently
// undecryptable dest spec (ErrSpecCorrupt) ends the reconcile as terminal
// `failed` — not an inconclusive retry — while mutating neither host. An
// undecryptable row never becomes readable, so retrying forever is wrong.
func TestReconcileMigrate_DestSpecCorrupt_Terminal(t *testing.T) {
	svc, fc, st := reconcileSvc(t)
	svc.SetStore(&getSpecErrStore{Memory: st, err: store.ErrSpecCorrupt})
	fc.AddPod("h1", healthyPod("web-x")) // source present
	fc.AddPod("h2", healthyPod("web-x")) // dest healthy

	resolved, ok, msg, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || ok {
		t.Fatalf("got resolved=%v ok=%v, want true/false (terminal failed)", resolved, ok)
	}
	if !strings.Contains(msg, "unreadable") {
		t.Fatalf("message = %q, want it to mention the spec is unreadable", msg)
	}
	// Neither host may be mutated on a terminal corrupt-spec result.
	if _, err := fc.PodInspect(context.Background(), "h2", "web-x"); err != nil {
		t.Fatalf("dest was deleted on a corrupt-spec result (data loss): %v", err)
	}
	if _, err := fc.PodInspect(context.Background(), "h1", "web-x"); err != nil {
		t.Fatalf("source was mutated on a corrupt-spec result: %v", err)
	}
}
```

If `strings` is not yet imported in `reconcile_test.go`, add it to the import block.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run TestReconcileMigrate_DestSpecCorrupt_Terminal ./internal/instance/ -v`
Expected: FAIL — the corrupt error currently classifies as inconclusive, so `resolved` is `false` (the `!resolved || ok` check fails).

- [ ] **Step 3: Replace `destSpecState` with a 4-state classifier**

In `internal/instance/reconcile.go`, replace the whole `destSpecState` function (lines 187–212, including the doc comment) with:

```go
// specState classifies a dest desired-state spec lookup.
type specState int

const (
	specInconclusive specState = iota // store could not be consulted (transient): retry
	specPersisted                     // spec row present and readable: roll forward
	specAbsent                        // definitively not persisted (ErrNotFound): fall through
	specCorrupt                       // permanently unreadable (ErrSpecCorrupt): terminal fail
)

// destSpecState reports whether the destination's desired-state spec was stored
// — the last durable step of a migrate's Apply (PlayKube → PutSpec → ingress). A
// healthy dest pod whose spec is missing means Apply was interrupted before it
// committed; that dest must NOT be treated as the source of truth.
//
//   - specInconclusive: the store could not be consulted (a transient error —
//     context cancellation or SQLITE_BUSY). The caller treats it as inconclusive
//     and retries; it must NEVER be read as "not persisted", which would wrongly
//     roll back (and delete) a committed destination.
//   - specPersisted: a readable spec row exists.
//   - specAbsent (store.ErrNotFound): definitively not persisted.
//   - specCorrupt (store.ErrSpecCorrupt): the row exists but will never decode
//     (decrypt failure after key loss/rotation, or a malformed JSON column). The
//     caller fails the job terminally — retrying is futile.
//
// TODO(#54): this re-derives Apply's commit point from "a spec row exists", and
// the ingress repair in the roll-forward branch is evidence the equivalence
// leaks (a durable step runs after PutSpec). A cleaner design records one
// explicit commit marker as Apply's final durable action and gates roll-forward
// on that fact.
func (s *Service) destSpecState(ctx context.Context, host, tmpl, slug string) specState {
	_, err := s.store.GetSpec(ctx, host, tmpl, slug)
	switch {
	case err == nil:
		return specPersisted
	case errors.Is(err, store.ErrNotFound):
		return specAbsent
	case errors.Is(err, store.ErrSpecCorrupt):
		return specCorrupt
	default:
		return specInconclusive
	}
}
```

- [ ] **Step 4: Update the caller**

In `internal/instance/reconcile.go`, replace the preamble at lines 100–107:

```go
		persisted, ok := s.destSpecState(ctx, req.ToHost, req.Template, req.Slug)
		if !ok {
			// The store could not be consulted (transient: cancellation, BUSY,
			// decrypt). Treat as inconclusive — NOT as "not persisted", which would
			// wrongly roll back and delete a committed destination.
			step("reconcile-inconclusive", "destination spec lookup failed")
			return false, false, "", nil
		}
		if persisted {
```

with:

```go
		spec := s.destSpecState(ctx, req.ToHost, req.Template, req.Slug)
		if spec == specInconclusive {
			// The store could not be consulted (transient: cancellation, BUSY).
			// Treat as inconclusive — NOT as "not persisted", which would wrongly
			// roll back and delete a committed destination.
			step("reconcile-inconclusive", "destination spec lookup failed")
			return false, false, "", nil
		}
		if spec == specCorrupt {
			// The dest spec row exists but will never decode (decrypt failure or a
			// malformed column). Retrying is futile — fail terminally.
			step("reconcile-spec-corrupt", "destination spec unreadable")
			return true, false, "destination spec unreadable (corrupt/undecryptable); manual cleanup required", nil
		}
		if spec == specPersisted {
```

The existing roll-forward block body (old lines 109–157) is unchanged and still ends with `return true, true, "", nil`; the trailing `// dest healthy but spec not persisted …` comment and the `if srcPresent {` rollback logic below it continue to handle `specAbsent` by fall-through.

- [ ] **Step 5: Correct the stale comment on the inconclusive test**

In `internal/instance/reconcile_test.go`, in `TestReconcileMigrate_DestSpecLookupError_Inconclusive` (and its doc comment ~lines 309–312), remove "decrypt" from the list of transient errors, since a decrypt failure is now `ErrSpecCorrupt` (terminal). Change the phrase `(BUSY / decrypt / cancellation)` to `(BUSY / cancellation)` and the comment line `// (BUSY / decrypt / cancellation) must be treated` accordingly. The test body (which uses a generic `SQLITE_BUSY` error, still inconclusive) does not change.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestReconcileMigrate -v`
Expected: PASS — the new corrupt-terminal test passes; all existing reconcile tests (incl. the inconclusive one) still pass.

- [ ] **Step 7: Commit**

```bash
git add internal/instance/reconcile.go internal/instance/reconcile_test.go
git commit -m "fix(reconcile): a corrupt/undecryptable dest spec fails terminally instead of retrying forever (#92, #96/#54)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `RotateInstanceSecrets` + `InstanceSecretState`

**Files:**
- Modify: `internal/instance/service.go` (add both methods after `UpgradeImage`, ~line 588)
- Test: `internal/instance/service_test.go` (reuses `newSvcMem`, `pgApply`, and the `getSpecErrStore` double from `reconcile_test.go` — same package)

- [ ] **Step 1: Write the failing tests**

Add to `internal/instance/service_test.go` (`require`, `assert`, `store`, `context` already imported):

```go
func TestRotateInstanceSecrets_OverlaysAndReapplies(t *testing.T) {
	svc, _, mem := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	// Seed a second per-instance secret directly so we can prove an absent name
	// is preserved across a single-secret rotation.
	sp, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	sp.Secrets["token"] = "keep"
	require.NoError(t, mem.PutSpec(ctx, sp))

	require.NoError(t, svc.RotateInstanceSecrets(ctx, "h1", "postgres", "demo",
		map[string]string{"password": "rotated"}))

	got, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.Equal(t, "rotated", got.Secrets["password"]) // overlaid
	assert.Equal(t, "keep", got.Secrets["token"])       // absent name preserved
	assert.Equal(t, "docker.io/library/postgres:16", got.Parameters["image"]) // params preserved
}

func TestRotateInstanceSecrets_EmptyIsRejected(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	require.Error(t, svc.RotateInstanceSecrets(ctx, "h1", "postgres", "demo", map[string]string{}))
}

func TestRotateInstanceSecrets_NoSpecIsNotFound(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	err := svc.RotateInstanceSecrets(context.Background(), "h1", "postgres", "ghost",
		map[string]string{"password": "x"})
	require.ErrorIs(t, err, ErrInstanceNotFound)
}

func TestRotateInstanceSecrets_CorruptSpecPropagates(t *testing.T) {
	svc, _, mem := newSvcMem(t)
	svc.SetStore(&getSpecErrStore{Memory: mem, err: store.ErrSpecCorrupt})
	err := svc.RotateInstanceSecrets(context.Background(), "h1", "postgres", "demo",
		map[string]string{"password": "x"})
	require.ErrorIs(t, err, store.ErrSpecCorrupt)
}

func TestInstanceSecretState_ReportsPresenceNotValues(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	set, err := svc.InstanceSecretState(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.True(t, set["password"]) // stored
	assert.False(t, set["token"])   // never set → absent → false
}

func TestInstanceSecretState_NoSpecIsNotFound(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	_, err := svc.InstanceSecretState(context.Background(), "h1", "postgres", "ghost")
	require.ErrorIs(t, err, ErrInstanceNotFound)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'TestRotateInstanceSecrets|TestInstanceSecretState' ./internal/instance/ -v`
Expected: FAIL — `undefined: svc.RotateInstanceSecrets` / `svc.InstanceSecretState`.

- [ ] **Step 3: Implement both methods**

In `internal/instance/service.go`, after `UpgradeImage` (ends ~line 588), add (`maps`, `errors`, `fmt`, `store` already imported):

```go
// RotateInstanceSecrets overlays newSecrets onto the instance's stored
// per-instance secrets and re-applies (Replace=true), restarting the pod. Names
// absent from newSecrets keep their existing value — callers are write-only and
// never see current values. An empty newSecrets is rejected so a blank submit
// does not pointlessly restart the instance. Returns ErrInstanceNotFound when no
// spec is stored, or the store's error (incl. store.ErrSpecCorrupt) when the
// spec cannot be read.
func (s *Service) RotateInstanceSecrets(ctx context.Context, host, tmpl, slug string, newSecrets map[string]string) error {
	if len(newSecrets) == 0 {
		return errors.New("no secrets to rotate")
	}
	spec, err := s.store.GetSpec(ctx, host, tmpl, slug)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ErrInstanceNotFound
		}
		return fmt.Errorf("load spec: %w", err)
	}
	merged := maps.Clone(spec.Secrets)
	if merged == nil {
		merged = map[string]string{}
	}
	for k, v := range newSecrets {
		merged[k] = v
	}
	return s.Apply(ctx, host, ApplyRequest{
		Template:   tmpl,
		Slug:       slug,
		Parameters: spec.Parameters,
		Secrets:    merged,
		Domains:    spec.Domains,
	}, ApplyOptions{Replace: true})
}

// InstanceSecretState reports, per stored per-instance secret name, that a value
// is present — presence only, never the value (the secret model is write-only).
// Names a template declares but the instance never set are simply absent from the
// map. Returns ErrInstanceNotFound when no spec is stored, or the store's error
// (incl. store.ErrSpecCorrupt) when the spec cannot be read.
func (s *Service) InstanceSecretState(ctx context.Context, host, tmpl, slug string) (map[string]bool, error) {
	spec, err := s.store.GetSpec(ctx, host, tmpl, slug)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, ErrInstanceNotFound
		}
		return nil, fmt.Errorf("load spec: %w", err)
	}
	set := make(map[string]bool, len(spec.Secrets))
	for name := range spec.Secrets {
		set[name] = true
	}
	return set, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'TestRotateInstanceSecrets|TestInstanceSecretState' ./internal/instance/ -v`
Expected: PASS (all six).

- [ ] **Step 5: Commit**

```bash
git add internal/instance/service.go internal/instance/service_test.go
git commit -m "feat(instance): RotateInstanceSecrets + write-only InstanceSecretState (#92)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `instance-detail` Manage-secrets button (+ Notice line)

**Files:**
- Modify: `internal/ui/handlers_instances.go` (`instanceView` signature + body; add `templatePerInstanceSecrets`; update `instanceDetail` and the two `lifecycle` calls)
- Modify: `internal/ui/handlers_deploy.go` (two `instanceView` calls: `deployCreate` line 275, `upgradeApply` line 323)
- Modify: `internal/ui/templates/instance-detail.html` (button + Notice)
- Test: `internal/ui/handlers_secrets_test.go` (created in this task; expanded in Task 5)

- [ ] **Step 1: Write the failing tests**

Create `internal/ui/handlers_secrets_test.go`:

```go
package ui

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// uiWithSecretInstance builds a UI with a running "demo/main" instance whose
// template declares two per-instance secrets ("password" set, "apikey" unset).
// Returns the backing store so rotation tests can assert on persisted secrets.
func uiWithSecretInstance(t *testing.T) (*UI, *store.Memory) {
	t.Helper()
	fc := fake.New()
	hosts := []config.Host{{ID: "edge-1"}}
	fc.AddPod("edge-1", podman.Pod{
		Name:   "demo-main",
		Status: "Running",
		Containers: []podman.Container{
			{Name: "demo-main-app", Image: "demo:1", Status: "Running"},
		},
	})
	mem := store.NewMemory()
	_ = mem.PutTemplate(context.Background(), store.Template{Meta: render.Meta{
		ID:         "demo",
		Parameters: []render.ParamDef{{Name: "image", Required: true}},
		Secrets:    render.Secrets{PerInstance: []string{"password", "apikey"}},
	}})
	_ = mem.PutSpec(context.Background(), store.Spec{
		Host: "edge-1", Template: "demo", Slug: "main",
		Parameters: map[string]any{"image": "demo:1"},
		Secrets:    map[string]string{"password": "stored"}, // apikey unset
	})
	svc := instance.NewService(fc, hosts)
	svc.SetStore(mem)
	hash, _ := config.HashToken("pw")
	u, err := New(Config{
		Svc:  svc,
		Jobs: mem,
		Auth: NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return u, mem
}

func TestInstanceDetailShowsManageSecretsWhenDeclared(t *testing.T) {
	u, _ := uiWithSecretInstance(t)
	body := authedGet(t, u, "/ui/hosts/edge-1/instances/demo/main").Body.String()
	if !strings.Contains(body, "/ui/hosts/edge-1/instances/demo/main/secrets") {
		t.Error("instance detail should link to the manage-secrets page when the template declares per-instance secrets")
	}
	if !strings.Contains(body, "Manage secrets") {
		t.Error("instance detail should show a 'Manage secrets' control")
	}
}

func TestInstanceDetailHidesManageSecretsWhenNoneDeclared(t *testing.T) {
	// uiWithStoredInstance's "demo" template declares no per-instance secrets.
	u := uiWithStoredInstance(t)
	body := authedGet(t, u, "/ui/hosts/edge-1/instances/demo/main").Body.String()
	if strings.Contains(body, "Manage secrets") {
		t.Error("instance detail must NOT show 'Manage secrets' when the template declares no per-instance secrets")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'TestInstanceDetail(Shows|Hides)ManageSecrets' ./internal/ui/ -v`
Expected: FAIL — `TestInstanceDetailShowsManageSecretsWhenDeclared` fails (no link rendered yet).

- [ ] **Step 3: Update `instanceView` and add the helper**

In `internal/ui/handlers_instances.go`, add `"context"` to the imports, then replace `instanceView` (lines 11–15) with:

```go
// instanceView builds the instance-detail render data. Upgrade is always
// available since the desired-state store is always present. HasSecrets gates
// the manage-secrets control on the template declaring any per-instance secrets.
func (u *UI) instanceView(ctx context.Context, host string, obs instance.Observed) map[string]any {
	return map[string]any{
		"Host":       host,
		"Inst":       obs,
		"CanUpgrade": true,
		"HasSecrets": len(u.templatePerInstanceSecrets(ctx, obs.Template)) > 0,
	}
}

// templatePerInstanceSecrets returns the per-instance secret names the template
// declares, or nil when the template is unknown or the catalog can't be read
// (best-effort: a lookup failure degrades to "no secrets", never an error here).
func (u *UI) templatePerInstanceSecrets(ctx context.Context, tmplID string) []string {
	tmpls, err := u.cfg.Svc.Templates(ctx)
	if err != nil {
		return nil
	}
	for _, t := range tmpls {
		if t.Meta.ID == tmplID {
			return t.Meta.Secrets.PerInstance
		}
	}
	return nil
}
```

- [ ] **Step 4: Thread `ctx` through the `instanceView` callers**

In `internal/ui/handlers_instances.go`:
- `instanceDetail` (line 24): `u.render(w, r, http.StatusOK, "instance-detail", u.pageData(u.instanceView(r.Context(), host, obs)))`
- `lifecycle` error path (line 59): `data := u.instanceView(ctx, host, obs)`
- `lifecycle` success path (line 78): `u.render(w, r, http.StatusOK, "instance-detail", u.pageData(u.instanceView(ctx, host, obs)))`

In `internal/ui/handlers_deploy.go`:
- `deployCreate` (line 275): `u.render(w, r, http.StatusOK, "instance-detail", u.pageData(u.instanceView(r.Context(), host, obs)))`
- `upgradeApply` (line 323): `u.render(w, r, http.StatusOK, "instance-detail", u.pageData(u.instanceView(r.Context(), host, obs)))`

- [ ] **Step 5: Add the button + Notice to the template**

In `internal/ui/templates/instance-detail.html`, change the top line (line 2) to also render a Notice:

```html
{{if .ActionError}}<div class="error">{{.ActionError}}</div>{{end}}
{{if .Notice}}<div class="notice">{{.Notice}}</div>{{end}}
```

And add the Manage-secrets control immediately after the Upgrade `<a>` (line 9), inside the `<span class="actions">`:

```html
    {{if .HasSecrets}}<a class="pure-button" href="/ui/hosts/{{.Host}}/instances/{{.Inst.Template}}/{{.Inst.Slug}}/secrets" hx-get="/ui/hosts/{{.Host}}/instances/{{.Inst.Template}}/{{.Inst.Slug}}/secrets" hx-target="#main" hx-push-url="true">Manage secrets</a>{{end}}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/ -v`
Expected: PASS — the two new tests pass and the existing UI suite still passes (the `instanceView` signature change is covered).

- [ ] **Step 7: Commit**

```bash
git add internal/ui/handlers_instances.go internal/ui/handlers_deploy.go internal/ui/templates/instance-detail.html internal/ui/handlers_secrets_test.go
git commit -m "feat(ui): show a Manage-secrets control on instances declaring per-instance secrets (#92)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Manage-secrets page (GET form + POST rotate)

**Files:**
- Create: `internal/ui/handlers_secrets.go`
- Modify: `internal/ui/ui.go` (register two routes)
- Create: `internal/ui/templates/secrets-form.html`
- Test: `internal/ui/handlers_secrets_test.go` (append; fixture from Task 4)

- [ ] **Step 1: Write the failing tests**

Append to `internal/ui/handlers_secrets_test.go` (imports `net/http`, `strings`, `context`, `url` may be needed — add `"net/url"` to the import block for the rotate tests):

```go
func TestSecretsFormListsDeclaredNamesWithStatus(t *testing.T) {
	u, _ := uiWithSecretInstance(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/demo/main/secrets")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `name="secret.password"`) || !strings.Contains(body, `name="secret.apikey"`) {
		t.Error("form should render a password input per declared per-instance secret")
	}
	if !strings.Contains(body, "(set)") || !strings.Contains(body, "(not set)") {
		t.Error("form should show set/not-set status (password set, apikey not set)")
	}
	if !strings.Contains(body, `hx-post="/ui/hosts/edge-1/instances/demo/main/secrets"`) {
		t.Error("the rotate form must POST (secrets must never ride a GET URL)")
	}
}

func TestSecretsFormUnknownHostIs404(t *testing.T) {
	u, _ := uiWithSecretInstance(t)
	w := authedGet(t, u, "/ui/hosts/nope/instances/demo/main/secrets")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestSecretsRotateRequiresCSRF(t *testing.T) {
	u, _ := uiWithSecretInstance(t)
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	form := url.Values{"secret.password": {"new"}} // no csrf_token
	r := httptest.NewRequest("POST", "/ui/hosts/edge-1/instances/demo/main/secrets", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (missing CSRF)", w.Code)
	}
}

func TestSecretsRotatePersistsNewValue(t *testing.T) {
	u, mem := uiWithSecretInstance(t)
	w := authedPost(t, u, "/ui/hosts/edge-1/instances/demo/main/secrets",
		url.Values{"secret.password": {"rotated"}})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	sp, err := mem.GetSpec(context.Background(), "edge-1", "demo", "main")
	if err != nil {
		t.Fatal(err)
	}
	if sp.Secrets["password"] != "rotated" {
		t.Errorf("password = %q, want rotated", sp.Secrets["password"])
	}
}

func TestSecretsRotateEmptyIsRejected(t *testing.T) {
	u, _ := uiWithSecretInstance(t)
	// authedPost adds csrf_token but no secret.* fields → nothing to rotate.
	w := authedPost(t, u, "/ui/hosts/edge-1/instances/demo/main/secrets", url.Values{})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (no secrets to rotate)", w.Code)
	}
	if !strings.Contains(w.Body.String(), `name="secret.password"`) {
		t.Error("an empty submit should re-render the form, not drop it")
	}
}

func TestSecretsFormCorruptSpecDegrades(t *testing.T) {
	u, mem := uiWithSecretInstance(t)
	u.cfg.Svc.SetStore(corruptSpecStore{mem})
	body := authedGet(t, u, "/ui/hosts/edge-1/instances/demo/main/secrets").Body.String()
	if !strings.Contains(body, "manual cleanup") {
		t.Error("a corrupt/undecryptable spec should degrade to a cleanup notice")
	}
	if strings.Contains(body, `type="password"`) {
		t.Error("no rotate inputs should render for a corrupt spec")
	}
}

// corruptSpecStore makes GetSpec report a permanently-undecryptable row, the way
// a key-loss/rotation leaves a sealed secrets blob.
type corruptSpecStore struct{ *store.Memory }

func (corruptSpecStore) GetSpec(context.Context, string, string, string) (store.Spec, error) {
	return store.Spec{}, store.ErrSpecCorrupt
}
```

Add `"net/http/httptest"` and `"net/url"` to the test file's import block (used by `TestSecretsRotateRequiresCSRF`).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'TestSecrets' ./internal/ui/ -v`
Expected: FAIL — routes/handlers/template don't exist yet (404s / `unknown template block "secrets-form"`).

- [ ] **Step 3: Create the handlers**

Create `internal/ui/handlers_secrets.go`:

```go
package ui

import (
	"context"
	"errors"
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

// secretsFormData builds the manage-secrets render data: the template's declared
// per-instance secret names and, per name, whether a value is currently stored
// (presence only — values are never read back). On a corrupt/undecryptable spec
// it sets Corrupt and omits Set so the form degrades to a cleanup notice.
func (u *UI) secretsFormData(ctx context.Context, host, tmpl, slug string) (map[string]any, error) {
	data := map[string]any{
		"Host":     host,
		"Template": tmpl,
		"Slug":     slug,
		"Names":    u.templatePerInstanceSecrets(ctx, tmpl),
	}
	set, err := u.cfg.Svc.InstanceSecretState(ctx, host, tmpl, slug)
	if err != nil {
		if errors.Is(err, store.ErrSpecCorrupt) {
			data["Corrupt"] = true
			return data, nil
		}
		return nil, err
	}
	data["Set"] = set
	return data, nil
}

// secretsForm (GET) renders the manage-secrets page for one instance.
func (u *UI) secretsForm(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	if !u.hostExists(host) {
		u.renderError(w, r, instance.ErrUnknownHost)
		return
	}
	data, err := u.secretsFormData(r.Context(), host, tmpl, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "secrets-form", u.pageData(data))
}

// secretsRotate (POST) rotates the submitted per-instance secrets and re-applies
// the instance. Secrets are read from the request body only — never the URL
// (#99). A blank submit re-renders the form at 400 rather than restarting the
// instance for no change.
func (u *UI) secretsRotate(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	if !u.hostExists(host) {
		u.renderError(w, r, instance.ErrUnknownHost)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	_, secrets := formValues(r.PostForm) // body-only; skips blanks
	if len(secrets) == 0 {
		data, derr := u.secretsFormData(r.Context(), host, tmpl, slug)
		if derr != nil {
			u.renderError(w, r, derr)
			return
		}
		data["Error"] = "enter a new value for at least one secret"
		u.render(w, r, http.StatusBadRequest, "secrets-form", u.pageData(data))
		return
	}
	if err := u.cfg.Svc.RotateInstanceSecrets(r.Context(), host, tmpl, slug, secrets); err != nil {
		data, derr := u.secretsFormData(r.Context(), host, tmpl, slug)
		if derr != nil {
			u.renderError(w, r, err) // can't even rebuild the form: surface the original error
			return
		}
		data["Error"] = err.Error()
		u.render(w, r, errorStatus(err), "secrets-form", u.pageData(data))
		return
	}
	obs, err := u.cfg.Svc.Get(r.Context(), host, tmpl, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	data := u.instanceView(r.Context(), host, obs)
	data["Notice"] = "Secrets rotated; instance re-applied."
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(data))
}
```

- [ ] **Step 4: Register the routes**

In `internal/ui/ui.go` `Handler()`, after the upgrade routes (line 94), add:

```go
	mux.Handle("GET /ui/hosts/{host}/instances/{template}/{slug}/secrets", guard(u.secretsForm))
	mux.Handle("POST /ui/hosts/{host}/instances/{template}/{slug}/secrets", guardW(u.secretsRotate))
```

(The literal `secrets` segment is more specific than the `{action}` lifecycle route, so Go 1.22's ServeMux routes it here with no conflict — same as the existing `upgrade` routes.)

- [ ] **Step 5: Create the template**

Create `internal/ui/templates/secrets-form.html`:

```html
{{define "secrets-form"}}
<h2>Manage secrets — {{.Template}} / {{.Slug}} on {{.Host}}</h2>
{{if .Error}}<p class="error">{{.Error}}</p>{{end}}
{{if .Corrupt}}
<p class="error">This instance's stored spec is unreadable (corrupt or undecryptable). Secret rotation is unavailable; manual cleanup is required.</p>
{{else}}
<p class="subtitle">Rotating a secret re-applies the instance and restarts it. Leave a field blank to keep its current value. Current values are never shown.</p>
<form hx-post="/ui/hosts/{{.Host}}/instances/{{.Template}}/{{.Slug}}/secrets" hx-target="#main" class="pure-form pure-form-stacked">
  <input type="hidden" name="csrf_token" value="{{.CSRF}}">
  {{range .Names}}
  <label>{{.}} {{if index $.Set .}}<span class="subtitle">(set)</span>{{else}}<span class="subtitle">(not set)</span>{{end}}
    <input type="password" name="secret.{{.}}" autocomplete="new-password" placeholder="new value">
  </label>
  {{end}}
  {{if not .Names}}<p class="subtitle">This template declares no per-instance secrets.</p>{{end}}
  <button type="submit" class="pure-button pure-button-primary">Rotate</button>
</form>
{{end}}
{{end}}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/ -v`
Expected: PASS — all `TestSecrets*` plus the Task 4 detail tests and the existing suite.

- [ ] **Step 7: Commit**

```bash
git add internal/ui/handlers_secrets.go internal/ui/ui.go internal/ui/templates/secrets-form.html internal/ui/handlers_secrets_test.go
git commit -m "feat(ui): manage-secrets page to view + rotate per-instance secrets (#92)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification (after all tasks)

- [ ] `make -C /home/tej/projects/podman-api/.worktrees/feat-92-secret-rotation test` — whole suite green.
- [ ] `gofmt -l internal/store internal/instance internal/ui` — empty (`.go` only).
- [ ] `go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/...` — clean.
- [ ] `make -C /home/tej/projects/podman-api/.worktrees/feat-92-secret-rotation build` — builds clean.
