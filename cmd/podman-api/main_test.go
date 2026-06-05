package main

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
	"github.com/iotready/podman-api/internal/ui"
	"github.com/iotready/podman-api/templates"
	"github.com/stretchr/testify/require"
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

func storeSpecFixture() store.Spec {
	return store.Spec{
		Host: "h1", Template: "postgres", Slug: "demo",
		Parameters: map[string]any{"image": "postgres:16"},
		Secrets:    map[string]string{"password": "p"},
	}
}

func TestOpenStore_KeyLess(t *testing.T) {
	// No key file: the store opens key-less. The template catalog and no-secret
	// specs work; secret ops are refused with store.ErrSecretsNeedKey.
	db := filepath.Join(t.TempDir(), "state.db")
	st, err := openStore(db, "")
	if err != nil {
		t.Fatalf("key-less openStore: %v", err)
	}
	if st == nil {
		t.Fatal("key-less store should return a non-nil store")
	}
	defer st.Close()
	noSecret := storeSpecFixture()
	noSecret.Secrets = nil
	if err := st.PutSpec(context.Background(), noSecret); err != nil {
		t.Fatalf("PutSpec (no secrets) on key-less store: %v", err)
	}
}

func TestOpenStore_CreatesParentDir(t *testing.T) {
	// openStore must MkdirAll the parent so a default path like
	// /var/lib/podman-api/state.db works on a fresh host.
	db := filepath.Join(t.TempDir(), "nested", "dir", "state.db")
	st, err := openStore(db, writeKey(t))
	if err != nil {
		t.Fatalf("openStore with nested parent: %v", err)
	}
	st.Close()
}

func TestOpenStore_BadKey(t *testing.T) {
	db := filepath.Join(t.TempDir(), "state.db")
	bad := filepath.Join(t.TempDir(), "bad.key")
	_ = os.WriteFile(bad, []byte("not-32-bytes"), 0o600)
	if _, err := openStore(db, bad); err == nil {
		t.Fatal("expected error for invalid key file")
	}
}

func TestOpenStore_Enabled(t *testing.T) {
	db := filepath.Join(t.TempDir(), "state.db")
	st, err := openStore(db, writeKey(t))
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	if st == nil {
		t.Fatal("enabled store should return a non-nil store")
	}
	if err := st.PutSpec(context.Background(), storeSpecFixture()); err != nil {
		t.Fatalf("PutSpec via returned store: %v", err)
	}
	if _, err := st.Enqueue(context.Background(), "migrate", nil, ""); err != nil {
		t.Fatalf("Enqueue via returned DB: %v", err)
	}
}

func TestSeedTemplates_OnEmptyOnly(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenSQLite(filepath.Join(t.TempDir(), "s.db"), nil)
	require.NoError(t, err)
	defer db.Close()
	n, err := seedTemplates(ctx, db, templates.Files)
	require.NoError(t, err)
	require.Positive(t, n)
	again, err := seedTemplates(ctx, db, templates.Files)
	require.NoError(t, err)
	require.Zero(t, again, "must not re-seed a populated store")
}

func TestSeedTemplates_RejectsMalformed(t *testing.T) {
	// A seed whose ingress names a container the body never declares passes
	// ParseMeta but must fail ValidateTemplate, so seedTemplates returns an
	// error and persists nothing (boot fails fast rather than storing garbage).
	//
	// The fixture pairs a VALID seed (alphabetically first) with the broken one
	// so a single validate+put loop would persist the good one before hitting the
	// bad one. seedTemplates is two-pass / all-or-nothing, so the store must stay
	// empty — otherwise the next boot (CountTemplates > 0) would never re-seed and
	// the catalog would be permanently partial (#61).
	ctx := context.Background()
	db, err := store.OpenSQLite(filepath.Join(t.TempDir(), "s.db"), nil)
	require.NoError(t, err)
	defer db.Close()

	seeds := fstest.MapFS{
		"a-good.yaml": &fstest.MapFile{Data: []byte(
			"# template-meta:\n" +
				"#   id: good\n" +
				"---\n" +
				"apiVersion: v1\n" +
				"kind: Pod\n" +
				"metadata:\n" +
				"  name: good\n" +
				"spec:\n" +
				"  containers:\n" +
				"    - name: app\n" +
				"      image: nginx:1\n",
		)},
		"b-broken.yaml": &fstest.MapFile{Data: []byte(
			"# template-meta:\n" +
				"#   id: broken\n" +
				"#   ingress:\n" +
				"#     container: missing\n" +
				"#     port: 8080\n" +
				"---\n" +
				"apiVersion: v1\n" +
				"kind: Pod\n" +
				"metadata:\n" +
				"  name: broken\n" +
				"spec:\n" +
				"  containers:\n" +
				"    - name: app\n" +
				"      image: nginx:1\n",
		)},
	}

	n, err := seedTemplates(ctx, db, seeds)
	require.Error(t, err)
	require.Zero(t, n)
	require.Contains(t, err.Error(), "broken")

	count, err := db.CountTemplates(ctx)
	require.NoError(t, err)
	require.Zero(t, count, "all-or-nothing: a single bad seed must persist nothing")
}

func TestComposeHandlerRootRedirectsToUI(t *testing.T) {
	svc := instance.NewService(fake.New(), []config.Host{{ID: "edge-1"}})
	hash, _ := config.HashToken("pw")
	uiApp, err := ui.New(ui.Config{
		Svc:  svc,
		Auth: ui.NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
	})
	if err != nil {
		t.Fatal(err)
	}
	h := composeHandler(http.NewServeMux(), uiApp)

	// Bare GET / → 303 to /ui.
	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/ui" {
		t.Fatalf("GET / → %d %q; want 303 /ui", w.Code, w.Header().Get("Location"))
	}

	// GET /ui without a session → 303 to /ui/login.
	r = httptest.NewRequest("GET", "/ui", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/ui/login" {
		t.Fatalf("GET /ui → %d %q; want 303 /ui/login", w.Code, w.Header().Get("Location"))
	}
}

func TestComposeHandlerNilUIReturnsAPIRouter(t *testing.T) {
	api := http.NewServeMux()
	api.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	h := composeHandler(api, nil)
	r := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("nil UI should pass through to API router, got %d", w.Code)
	}
}
