package ui

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/instance"
)

func TestRenderFullPageVsFragment(t *testing.T) {
	u, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}

	// Full page (no HX-Request) → includes <!DOCTYPE>.
	full := httptest.NewRecorder()
	u.render(full, httptest.NewRequest("GET", "/ui/login", nil), "login", map[string]any{})
	if !strings.Contains(full.Body.String(), "<!DOCTYPE html>") {
		t.Error("full page should include the layout")
	}

	// HX-Request → fragment only, no layout.
	frag := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/ui/login", nil)
	r.Header.Set("HX-Request", "true")
	u.render(frag, r, "login", map[string]any{})
	if strings.Contains(frag.Body.String(), "<!DOCTYPE html>") {
		t.Error("HTMX fragment must not include the layout")
	}
}

func TestErrorStatus(t *testing.T) {
	cases := map[error]int{
		instance.ErrUnknownHost:    http.StatusNotFound,
		instance.ErrInstanceExists: http.StatusConflict,
		instance.ErrHostDraining:   http.StatusConflict,
		errors.New("boom"):         http.StatusInternalServerError,
	}
	for err, want := range cases {
		if got := errorStatus(err); got != want {
			t.Errorf("errorStatus(%v) = %d, want %d", err, got, want)
		}
	}
}
