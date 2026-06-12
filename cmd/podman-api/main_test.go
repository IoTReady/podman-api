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
	"github.com/iotready/podman-api/server"
	"github.com/iotready/podman-api/templates"
	"github.com/stretchr/testify/require"
)

func TestJobRegistry_IncludesBackupKinds(t *testing.T) {
	svc := instance.NewService(fake.New(), nil)
	reg, recs := server.BuildJobRegistry(svc, nil, nil, 1, nil, nil)

	for _, kind := range []string{"migrate", "evacuate", "prune", "backup", "restore"} {
		if _, ok := reg[kind]; !ok {
			t.Errorf("registry missing kind %q", kind)
		}
	}
	for _, kind := range []string{"migrate", "backup"} {
		if _, ok := recs[kind]; !ok {
			t.Errorf("reconcilers missing kind %q", kind)
		}
	}
}

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
	db := filepath.Join(t.TempDir(), "state.db")
	st, err := server.OpenStore(db, "")
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
	db := filepath.Join(t.TempDir(), "nested", "dir", "state.db")
	st, err := server.OpenStore(db, writeKey(t))
	if err != nil {
		t.Fatalf("openStore with nested parent: %v", err)
	}
	st.Close()
}

func TestOpenStore_BadKey(t *testing.T) {
	db := filepath.Join(t.TempDir(), "state.db")
	bad := filepath.Join(t.TempDir(), "bad.key")
	_ = os.WriteFile(bad, []byte("not-32-bytes"), 0o600)
	if _, err := server.OpenStore(db, bad); err == nil {
		t.Fatal("expected error for invalid key file")
	}
}

func TestOpenStore_Enabled(t *testing.T) {
	db := filepath.Join(t.TempDir(), "state.db")
	st, err := server.OpenStore(db, writeKey(t))
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
	n, err := server.SeedTemplates(ctx, db, templates.Files)
	require.NoError(t, err)
	require.Positive(t, n)
	again, err := server.SeedTemplates(ctx, db, templates.Files)
	require.NoError(t, err)
	require.Zero(t, again, "must not re-seed a populated store")
}

func TestSeedTemplates_RejectsMalformed(t *testing.T) {
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

	n, err := server.SeedTemplates(ctx, db, seeds)
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
	h := server.ComposeHandler(http.NewServeMux(), uiApp)

	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/ui" {
		t.Fatalf("GET / → %d %q; want 303 /ui", w.Code, w.Header().Get("Location"))
	}

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
	h := server.ComposeHandler(api, nil)
	r := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("nil UI should pass through to API router, got %d", w.Code)
	}
}
