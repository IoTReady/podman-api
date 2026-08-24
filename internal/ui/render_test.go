package ui

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/api"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

func TestRenderFullPageVsFragment(t *testing.T) {
	u, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}

	// Full page (no HX-Request) → includes <!DOCTYPE>.
	full := httptest.NewRecorder()
	u.render(full, httptest.NewRequest("GET", "/ui/login", nil), http.StatusOK, "login", map[string]any{})
	if !strings.Contains(full.Body.String(), "<!DOCTYPE html>") {
		t.Error("full page should include the layout")
	}

	// HX-Request → fragment only, no layout.
	frag := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/ui/login", nil)
	r.Header.Set("HX-Request", "true")
	u.render(frag, r, http.StatusOK, "login", map[string]any{})
	if strings.Contains(frag.Body.String(), "<!DOCTYPE html>") {
		t.Error("HTMX fragment must not include the layout")
	}
}

// TestRenderFragmentThenFullPage reproduces the v1.0.18 regression: an HTMX
// fragment render must not poison the shared template set for later full-page
// renders. html/template forbids Clone() after Execute(), so if the fragment
// path executes the master template directly, every subsequent full-page render
// (which clones the master to redefine "body") 500s with "cannot Clone after it
// has executed". htmx-boosted navigation makes the fragment request come first
// in the wild, so this order — fragment, then full page — is the one that broke
// production while the safe order in TestRenderFullPageVsFragment stayed green.
func TestRenderFragmentThenFullPage(t *testing.T) {
	u, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}

	// HX-Request first (this is what an htmx-boosted navigation sends).
	frag := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/ui/login", nil)
	r.Header.Set("HX-Request", "true")
	u.render(frag, r, http.StatusOK, "login", map[string]any{})
	if frag.Code != http.StatusOK {
		t.Fatalf("fragment render: status = %d, want 200", frag.Code)
	}

	// Full page after — must still succeed (not 500 on a poisoned Clone()).
	full := httptest.NewRecorder()
	u.render(full, httptest.NewRequest("GET", "/ui/login", nil), http.StatusOK, "login", map[string]any{})
	if full.Code != http.StatusOK {
		t.Fatalf("full page after fragment: status = %d, want 200", full.Code)
	}
	if !strings.Contains(full.Body.String(), "<!DOCTYPE html>") {
		t.Error("full page should include the layout")
	}
}

func TestRenderUnknownBlockIs500(t *testing.T) {
	u, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	u.render(w, httptest.NewRequest("GET", "/ui", nil), http.StatusOK, "does-not-exist", nil)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("unknown block: status = %d, want 500", w.Code)
	}
}

func TestLayoutCacheBustsAssets(t *testing.T) {
	u, err := New(Config{Version: "v1.2.3"})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/ui/login", nil) // full-page (no HX-Request) → layout
	u.render(rec, req, http.StatusOK, "login", map[string]any{})
	body := rec.Body.String()
	for _, asset := range []string{"/ui/static/app.css?v=v1.2.3", "/ui/static/htmx.min.js?v=v1.2.3"} {
		if !strings.Contains(body, asset) {
			t.Errorf("layout missing cache-busted asset %q\nbody:\n%s", asset, body)
		}
	}
}

func TestErrorStatus(t *testing.T) {
	cases := map[error]int{
		instance.ErrUnknownHost:         http.StatusNotFound,
		instance.ErrUnknownTemplate:     http.StatusNotFound,
		instance.ErrInstanceNotFound:    http.StatusNotFound,
		instance.ErrInstanceExists:      http.StatusConflict,
		instance.ErrPortConflict:        http.StatusConflict,
		instance.ErrNetworkNameConflict: http.StatusConflict,
		instance.ErrHostDraining:        http.StatusLocked,
		instance.ErrHostSecretMissing:   http.StatusUnprocessableEntity,
		instance.ErrImagePull:           http.StatusBadGateway,
		instance.ErrStoreDisabled:       http.StatusNotImplemented,
		instance.ErrSameHost:            http.StatusBadRequest,
		store.ErrSecretsNeedKey:         http.StatusBadRequest,
		store.ErrSecretsUndecryptable:   http.StatusUnprocessableEntity,
		instance.ErrInvalidBackupScope:  http.StatusBadRequest,
		errors.New("boom"):              http.StatusInternalServerError,
	}
	for err, want := range cases {
		if got := errorStatus(err); got != want {
			t.Errorf("errorStatus(%v) = %d, want %d", err, got, want)
		}
	}
}

// TestErrorStatus_AgreesWithAPIClassify locks the two parallel switches
// together. errorStatus (here) and the JSON API's classify()
// (internal/api/errors.go) are hand-maintained mirrors of each other, and they
// drift silently: ErrInvalidBackupScope was added to classify() as a 400 and
// missed here, so a plain client-side scope error rendered the UI page with a
// 500 — enough to trip uptime monitors on a benign action. Any sentinel added
// to one switch and not the other fails this test rather than waiting to be
// noticed in production. classify() is unexported, so its status is read the
// only way a caller can: through the exported WriteError.
func TestErrorStatus_AgreesWithAPIClassify(t *testing.T) {
	shared := []error{
		instance.ErrUnknownHost,
		instance.ErrUnknownTemplate,
		instance.ErrInstanceNotFound,
		instance.ErrInstanceExists,
		instance.ErrPortConflict,
		instance.ErrNetworkNameConflict,
		instance.ErrHostDraining,
		instance.ErrHostSecretMissing,
		instance.ErrImagePull,
		instance.ErrStoreDisabled,
		instance.ErrSameHost,
		instance.ErrBackupNotFound,
		instance.ErrBackupNotRestorable,
		instance.ErrBackupBusy,
		instance.ErrBackupsDisabled,
		instance.ErrInvalidBackupScope,
		store.ErrNotFound,
		store.ErrSecretsNeedKey,
		store.ErrSecretsUndecryptable,
		store.ErrSpecCorrupt,
	}
	for _, err := range shared {
		rec := httptest.NewRecorder()
		api.WriteError(rec, err)
		if got, want := errorStatus(err), rec.Code; got != want {
			t.Errorf("errorStatus(%v) = %d, but api.classify() = %d — the two switches have drifted", err, got, want)
		}
	}
}
