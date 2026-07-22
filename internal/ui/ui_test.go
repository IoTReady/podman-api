package ui

import (
	"net/http"
	"net/http/httptest"
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
