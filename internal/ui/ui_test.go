package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewParsesTemplates(t *testing.T) {
	u, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, name := range []string{"layout", "login"} {
		if u.tmpl.Lookup(name) == nil {
			t.Errorf("template %q not parsed", name)
		}
	}
}

func TestStaticHandlerCaching(t *testing.T) {
	u, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	h := u.staticHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/ui/static/app.css", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "public, max-age=31536000, immutable" {
		t.Errorf("Cache-Control = %q", cc)
	}
	etag := rec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("missing ETag")
	}

	// A conditional request with the same ETag must 304.
	req2 := httptest.NewRequest("GET", "/ui/static/app.css", nil)
	req2.Header.Set("If-None-Match", etag)
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusNotModified {
		t.Errorf("conditional status = %d, want 304", rec2.Code)
	}
}

func TestRedesignAssets(t *testing.T) {
	// app.css carries the design system + @font-face; pure-min.css is gone.
	css, err := staticFS.ReadFile("static/app.css")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"@font-face", "IBM Plex Sans", "--accent:#F5A623", "data-theme=\"dark\"", ".badge", ".card", ".btn"} {
		if !strings.Contains(string(css), want) {
			t.Errorf("app.css missing %q", want)
		}
	}
	if _, err := staticFS.ReadFile("static/pure-min.css"); err == nil {
		t.Error("pure-min.css should be deleted")
	}
	for _, f := range []string{"static/fonts/ibm-plex-sans-400.woff2", "static/fonts/ibm-plex-mono-400.woff2"} {
		if _, err := staticFS.ReadFile(f); err != nil {
			t.Errorf("missing embedded font %s: %v", f, err)
		}
	}
	// layout no longer links pure-min.css.
	u, err := New(Config{Version: "t"})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	u.render(rec, httptest.NewRequest("GET", "/ui/login", nil), http.StatusOK, "login", map[string]any{})
	if strings.Contains(rec.Body.String(), "pure-min.css") {
		t.Error("layout still links pure-min.css")
	}
}
