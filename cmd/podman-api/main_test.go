package main

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
	"github.com/iotready/podman-api/internal/ui"
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

func TestOpenStore_Disabled(t *testing.T) {
	st, err := openStore("", "")
	if err != nil || st != nil {
		t.Fatalf("disabled store should be (nil,nil), got (%v,%v)", st, err)
	}
}

func TestOpenStore_MissingKey(t *testing.T) {
	db := filepath.Join(t.TempDir(), "state.db")
	if _, err := openStore(db, ""); err == nil {
		t.Fatal("expected error when -state-db set without -spec-key-file")
	}
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

func TestComposeHandlerRootRedirectsToUI(t *testing.T) {
	svc := instance.NewService(fake.New(), []config.Host{{ID: "edge-1"}}, nil)
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
