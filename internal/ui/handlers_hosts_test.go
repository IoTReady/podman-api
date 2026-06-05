package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
)

// uiWithService builds a UI backed by a real instance.Service over the fake
// podman client, with one host "edge-1" and an authenticated operator.
func uiWithService(t *testing.T) *UI {
	t.Helper()
	fc := fake.New()
	hosts := []config.Host{{ID: "edge-1"}}
	svc := instance.NewService(fc, hosts, nil)
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

// authedGet drives a GET through a real session.
func authedGet(t *testing.T, u *UI, path string) *httptest.ResponseRecorder {
	t.Helper()
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	r := httptest.NewRequest("GET", path, nil)
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	return w
}

func TestDashboardListsHosts(t *testing.T) {
	u := uiWithService(t)
	w := authedGet(t, u, "/ui")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "edge-1") {
		t.Error("dashboard should list host edge-1")
	}
}

func TestZeroHostsStillRendersShell(t *testing.T) {
	svc := instance.NewService(fake.New(), nil, nil) // zero configured hosts
	hash, _ := config.HashToken("pw")
	u, err := New(Config{
		Svc:  svc,
		Auth: NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
	})
	if err != nil {
		t.Fatal(err)
	}
	w := authedGet(t, u, "/ui")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "Sign out") {
		t.Error("an authenticated page with zero hosts should still render the shell chrome")
	}
}

func TestHostInstancesUnknownHostIs404(t *testing.T) {
	u := uiWithService(t)
	w := authedGet(t, u, "/ui/hosts/does-not-exist")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for unknown host", w.Code)
	}
	// A full-page authenticated error still renders the shell (sidebar hosts),
	// not a chrome-free naked error.
	if !strings.Contains(w.Body.String(), "edge-1") {
		t.Error("full-page error should keep the sidebar shell")
	}
}

func TestHostInstancesPageRenders(t *testing.T) {
	u := uiWithService(t)
	w := authedGet(t, u, "/ui/hosts/edge-1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "edge-1") {
		t.Error("host page should name the host")
	}
	if !strings.Contains(body, "Deploy") {
		t.Error("host page should have a Deploy action")
	}
}
