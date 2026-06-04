package ui

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/config"
)

func testUI(t *testing.T) *UI {
	t.Helper()
	hash, _ := config.HashToken("pw")
	u, err := New(Config{
		Auth: NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestLoginSuccessSetsCookie(t *testing.T) {
	u := testUI(t)
	form := url.Values{"username": {"op"}, "password": {"pw"}}
	r := httptest.NewRequest("POST", "/ui/login", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)

	if w.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want 303", w.Code)
	}
	if loc := w.Header().Get("Location"); loc != "/ui" {
		t.Errorf("redirect = %q, want /ui", loc)
	}
	if !strings.Contains(w.Header().Get("Set-Cookie"), sessionCookie) {
		t.Error("expected session cookie")
	}
}

func TestLoginFailureRerendersForm(t *testing.T) {
	u := testUI(t)
	form := url.Values{"username": {"op"}, "password": {"WRONG"}}
	r := httptest.NewRequest("POST", "/ui/login", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
	if !strings.Contains(w.Body.String(), "invalid credentials") {
		t.Error("expected error message in re-rendered form")
	}
}

func TestCSRFGuardedPostRejectedWithoutToken(t *testing.T) {
	u := testUI(t)
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	r := httptest.NewRequest("POST", "/ui/logout", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (missing CSRF token)", w.Code)
	}
}

func TestCSRFGuardedPostAcceptedWithToken(t *testing.T) {
	u := testUI(t)
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	r := httptest.NewRequest("POST", "/ui/logout", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	r.Header.Set(csrfHeader, csrfToken(tok))
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/ui/login" {
		t.Fatalf("got %d %q; want 303 /ui/login", w.Code, w.Header().Get("Location"))
	}
	if _, ok := u.cfg.Sessions.Lookup(tok); ok {
		t.Error("logout should have deleted the session")
	}
}

func TestProtectedRouteRedirectsWhenUnauthenticated(t *testing.T) {
	u := testUI(t)
	r := httptest.NewRequest("GET", "/ui", nil)
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/ui/login" {
		t.Fatalf("got %d %q; want 303 /ui/login", w.Code, w.Header().Get("Location"))
	}
}
