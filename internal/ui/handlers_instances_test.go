package ui

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
)

// uiWithSeededInstance builds a UI with a "postgres" template registered and a
// running postgres-main pod seeded in the fake, so instanceDetail can render a
// real instance.
func uiWithSeededInstance(t *testing.T) *UI {
	t.Helper()
	fc := fake.New()
	hosts := []config.Host{{ID: "edge-1"}}
	tmpls := []config.Template{
		{Meta: render.Meta{ID: "postgres"}},
	}
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
	svc := instance.NewService(fc, hosts, tmpls)
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

// authedAction drives an authenticated POST with a valid CSRF token.
func authedAction(t *testing.T, u *UI, path string) *httptest.ResponseRecorder {
	t.Helper()
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	form := url.Values{csrfField: {csrfToken(tok)}}
	r := httptest.NewRequest("POST", path, strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	return w
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

func TestLogsTailRendersLines(t *testing.T) {
	u := uiWithSeededInstance(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "database system is ready") {
		t.Error("logs view should show the seeded log line")
	}
}

func TestInstanceDetailRendersSeededInstance(t *testing.T) {
	u := uiWithSeededInstance(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "postgres / main") {
		t.Error("detail page should show template/slug header")
	}
	if !strings.Contains(body, "Running") {
		t.Error("detail page should show pod status")
	}
	if !strings.Contains(body, "postgres-main-db") {
		t.Error("detail page should list the container")
	}
}
