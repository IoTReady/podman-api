package ui

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// uiWithSeededInstance builds a UI with a "postgres" template registered and a
// running postgres-main pod seeded in the fake, so instanceDetail can render a
// real instance.
func uiWithSeededInstance(t *testing.T) *UI {
	t.Helper()
	fc := fake.New()
	hosts := []config.Host{{ID: "edge-1"}}
	fc.AddPod("edge-1", podman.Pod{
		Name:   "postgres-main",
		Status: "Running",
		Labels: map[string]string{
			"podman-api/template": "postgres",
			"podman-api/slug":     "main",
		},
		Containers: []podman.Container{
			{Name: "postgres-main-db", Image: "docker.io/library/postgres", Status: "Running"},
		},
	})
	fc.LogLines = []podman.LogLine{{Container: "postgres-main-db", Line: "database system is ready"}}
	mem := store.NewMemory()
	_ = mem.PutTemplate(context.Background(), store.Template{Meta: render.Meta{ID: "postgres"}})
	svc := instance.NewService(fc, hosts)
	svc.SetStore(mem)
	hash, _ := config.HashToken("pw")
	u, err := New(Config{
		Svc:  svc,
		Auth: NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// authedAction drives an authenticated POST (no extra fields) with a valid CSRF
// token. It is authedPost with an empty body — see authedPost in
// handlers_deploy_test.go.
func authedAction(t *testing.T, u *UI, path string) *httptest.ResponseRecorder {
	t.Helper()
	return authedPost(t, u, path, nil)
}

func TestLifecycleFailureKeepsDetailWithBanner(t *testing.T) {
	fc := fake.New()
	fc.AddPod("edge-1", podman.Pod{
		Name:   "postgres-main",
		Status: "Running",
		Containers: []podman.Container{
			{Name: "postgres-main-db", Image: "postgres:16", Status: "Running"},
		},
	})
	fc.LifecycleErr = errors.New("daemon refused")
	mem := store.NewMemory()
	_ = mem.PutTemplate(context.Background(), store.Template{Meta: render.Meta{ID: "postgres"}})
	svc := instance.NewService(fc, []config.Host{{ID: "edge-1"}})
	svc.SetStore(mem)
	hash, _ := config.HashToken("pw")
	u, err := New(Config{
		Svc:  svc,
		Auth: NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
	})
	if err != nil {
		t.Fatal(err)
	}

	w := authedAction(t, u, "/ui/hosts/edge-1/instances/postgres/main/stop")
	body := w.Body.String()
	// The detail panel stays in place (action buttons still present)...
	if !strings.Contains(body, "Restart") {
		t.Error("a failed lifecycle action should keep the detail panel, not replace it with a bare error")
	}
	// ...with the failure shown as a banner.
	if !strings.Contains(body, "daemon refused") {
		t.Error("a failed lifecycle action should surface the error as a banner")
	}
}

func TestLifecycleUnknownActionIs400(t *testing.T) {
	u := uiWithService(t)
	w := authedAction(t, u, "/ui/hosts/edge-1/instances/postgres/main/frobnicate")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for unknown action", w.Code)
	}
}

func TestLifecycleMissingCSRFIs403(t *testing.T) {
	u := uiWithService(t)
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op"})
	r := httptest.NewRequest("POST", "/ui/hosts/edge-1/instances/postgres/main/stop", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (no csrf token)", w.Code)
	}
}

func TestInstanceDetailNotFoundIs404(t *testing.T) {
	u := uiWithService(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/ghost")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for missing instance", w.Code)
	}
}

// TestLifecycleDeleteRendersFullHostViewModel guards against a regression
// where the delete branch re-rendered host-instances with a partial view
// model (missing AgeSeconds/Unreachable/Cold), producing the literal string
// "<no value>" in the freshness line instead of the polling fragment markup.
func TestLifecycleDeleteRendersFullHostViewModel(t *testing.T) {
	u := uiWithSeededInstance(t)
	w := authedAction(t, u, "/ui/hosts/edge-1/instances/postgres/main/delete")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if strings.Contains(body, "<no value>") {
		t.Error("host-instances view model after delete is incomplete: body contains the literal \"<no value>\" bug signature")
	}
	if !strings.Contains(body, `hx-get="/ui/hosts/edge-1/fragment"`) {
		t.Error("host-instances after delete should render the full shell with the polling fragment (hx-get to /fragment)")
	}
}

func TestInstanceDetailRendersSeededInstance(t *testing.T) {
	u := uiWithSeededInstance(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "postgres") || !strings.Contains(body, "main") {
		t.Error("detail page should show template/slug header")
	}
	if !strings.Contains(body, "Running") {
		t.Error("detail page should show pod status")
	}
	if !strings.Contains(body, "postgres-main-db") {
		t.Error("detail page should list the container")
	}
}
